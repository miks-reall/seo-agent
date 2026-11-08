import asyncio
import json
import re
import aiohttp
from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from sqlalchemy import select
from core.config import cfg
from core.db import async_session, init_db
from core.models import Post, Media
from core.redis_client import redis, ensure_group
from core.utils import is_ad_or_spam, clean_text, calc_simhash, is_near_duplicate

GROUP_NAME = "processor_group"
CONSUMER_NAME = "worker_1"

SYSTEM_PROMPT = """Ты редактор новостной ленты. Перепиши исходный текст новости.
Правила:
1. Убери воду, кликбейт, призывы подписаться и рекламные вставки.
2. Изложи суть плотно, интересно и по делу. После основного абзаца выдели 2-3 ключевых тезиса короткими пунктами через дефис.
3. Никаких эмодзи и смайликов в тексте.
4. Верни ответ строго в формате валидного JSON с двумя ключами:
{"text": "переписанный текст", "tags": ["тег1", "тег2", "тег3"]}
Теги пиши на русском языке в нижнем регистре без символа решетки."""

bot = Bot(token=cfg.TG_BOT_TOKEN)
JSON_CLEAN_RE = re.compile(r"^```(?:json)?\s*|\s*```$", re.MULTILINE)


async def rewrite_via_llm(http_session: aiohttp.ClientSession, raw_text: str) -> dict | None:
    url = f"{cfg.LLM_BASE_URL}/chat/completions"
    headers = {
        "Authorization": f"Bearer {cfg.LLM_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": cfg.LLM_MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Исходный текст:\n{raw_text}"},
        ],
        "response_format": {"type": "json_object"},
        "temperature": 0.3,
    }

    # делаем 3 попытки на случай сетевых затыков или 429 от облака
    for attempt in range(1, 4):
        try:
            async with http_session.post(url, headers=headers, json=payload, timeout=60) as resp:
                if resp.status == 429 or resp.status >= 500:
                    await asyncio.sleep(3 * attempt)
                    continue
                if resp.status != 200:
                    err_body = await resp.text()
                    print(f"Ошибка API LLM ({resp.status}): {err_body}")
                    return None

                data = await resp.json()
                content = data["choices"][0]["message"]["content"]
                # срезаем ```json на случай если модель проигнорировала строгий режим
                cleaned_json = JSON_CLEAN_RE.sub("", content).strip()
                return json.loads(cleaned_json)

        except Exception as e:
            print(f"Сбой запроса к облачной LLM (попытка {attempt}/3): {e}")
            await asyncio.sleep(2 * attempt)

    return None


def build_mod_kb(post_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="В публикацию", callback_data=f"pub:{post_id}"),
                InlineKeyboardButton(text="Править текст", callback_data=f"edit:{post_id}"),
            ],
            [
                InlineKeyboardButton(text="В мусорку", callback_data=f"drop:{post_id}"),
            ],
        ]
    )


async def process_item(http_session: aiohttp.ClientSession, payload: dict):
    raw_text = payload.get("text", "")
    if is_ad_or_spam(raw_text):
        return

    cleaned = clean_text(raw_text)
    if len(cleaned) < 60:
        return

    shash = calc_simhash(cleaned)

    async with async_session() as session:
        stmt = select(Post.simhash).order_by(Post.id.desc()).limit(500)
        recent_hashes = (await session.execute(stmt)).scalars().all()

        if any(is_near_duplicate(shash, old_h) for old_h in recent_hashes):
            print("Отсеян дубликат по SimHash")
            return

        ai_res = await rewrite_via_llm(http_session, cleaned)
        if not ai_res or not ai_res.get("text"):
            return

        raw_tags = ai_res.get("tags", [])
        tags_str = " ".join(
            f"#{t.strip().replace(' ', '_')}" for t in raw_tags if t.strip()
        )

        post = Post(
            source_chat=payload.get("source", "unknown"),
            orig_text=cleaned,
            ai_text=ai_res["text"].strip(),
            tags=tags_str,
            simhash=shash,
            status="draft",
        )
        session.add(post)
        await session.flush()

        for m in payload.get("media", []):
            session.add(
                Media(
                    post_id=post.id,
                    media_type=m["type"],
                    file_path=m["path"],
                )
            )

        await session.commit()

        preview = (
            f"Новый материал на проверку (ID {post.id})\n"
            f"Источник: {post.source_chat}\n"
            f"Вложений: {len(payload.get('media', []))}\n\n"
            f"{post.formatted_text()}"
        )
        await bot.send_message(
            chat_id=cfg.ADMIN_CHAT_ID,
            text=preview[:4090],
            reply_markup=build_mod_kb(post.id),
        )


async def main():
    await init_db()
    await ensure_group(cfg.STREAM_RAW, GROUP_NAME)
    print("Воркер обработки (Cloud LLM) запущен...")

    async with aiohttp.ClientSession() as http_session:
        while True:
            try:
                entries = await redis.xreadgroup(
                    GROUP_NAME,
                    CONSUMER_NAME,
                    {cfg.STREAM_RAW: ">"},
                    count=1,
                    block=5000,
                )
                if not entries:
                    continue

                for _, messages in entries:
                    for msg_id, fields in messages:
                        data = json.loads(fields["data"])
                        await process_item(http_session, data)
                        await redis.xack(cfg.STREAM_RAW, GROUP_NAME, msg_id)
            except Exception as e:
                print(f"Сбой в цикле обработки: {e}")
                await asyncio.sleep(2)


if __name__ == "__main__":
    asyncio.run(main())