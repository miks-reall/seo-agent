import asyncio
import json
from aiogram import Bot
from aiogram.types import FSInputFile, InputMediaPhoto, InputMediaVideo
from aiogram.exceptions import TelegramRetryAfter
from core.config import cfg
from core.db import async_session
from core.models import Post
from core.redis_client import redis, ensure_group, acquire_token

GROUP_NAME = "tg_pub_group"
CONSUMER_NAME = "tg_worker_1"

bot = Bot(token=cfg.TG_BOT_TOKEN)


async def publish_to_tg(post_id: int):
    # лимит 0.3 токена в секунду (~18 постов в минуту) для защиты от бана канала
    await acquire_token("tg_channel_limit", rate=0.3, capacity=1)

    async with async_session() as session:
        post = await session.get(Post, post_id)
        if not post:
            return

        text = post.formatted_text()
        media_items = post.media

        if not media_items:
            await bot.send_message(chat_id=cfg.TARGET_TG_CHANNEL_ID, text=text[:4096])

        elif len(media_items) == 1:
            m = media_items[0]
            file_arg = m.tg_file_id if m.tg_file_id else FSInputFile(m.file_path)

            if m.media_type == "photo":
                res = await bot.send_photo(
                    chat_id=cfg.TARGET_TG_CHANNEL_ID,
                    photo=file_arg,
                    caption=text[:1024],
                )
                if not m.tg_file_id and res.photo:
                    m.tg_file_id = res.photo[-1].file_id
            else:
                res = await bot.send_video(
                    chat_id=cfg.TARGET_TG_CHANNEL_ID,
                    video=file_arg,
                    caption=text[:1024],
                )
                if not m.tg_file_id and res.video:
                    m.tg_file_id = res.video.file_id

        else:
            media_group = []
            for idx, m in enumerate(media_items[:10]):
                file_arg = m.tg_file_id if m.tg_file_id else FSInputFile(m.file_path)
                cap = text[:1024] if idx == 0 else None
                if m.media_type == "photo":
                    media_group.append(InputMediaPhoto(media=file_arg, caption=cap))
                else:
                    media_group.append(InputMediaVideo(media=file_arg, caption=cap))

            msgs = await bot.send_media_group(
                chat_id=cfg.TARGET_TG_CHANNEL_ID, media=media_group
            )
            # сохраняем полученные file_id в базу
            for m_obj, sent_msg in zip(media_items[:10], msgs):
                if not m_obj.tg_file_id:
                    if sent_msg.photo:
                        m_obj.tg_file_id = sent_msg.photo[-1].file_id
                    elif sent_msg.video:
                        m_obj.tg_file_id = sent_msg.video.file_id

        post.status = "published"
        await session.commit()


async def handle_failure(payload: dict, error_msg: str):
    attempt = payload.get("attempt", 1)
    post_id = payload["post_id"]

    if attempt < 3:
        await asyncio.sleep(5 * attempt)
        payload["attempt"] = attempt + 1
        await redis.xadd(cfg.STREAM_PUB_TG, {"data": json.dumps(payload)})
    else:
        await redis.xadd(
            cfg.STREAM_DLQ,
            {"platform": "tg", "post_id": str(post_id), "error": error_msg},
        )
        await bot.send_message(
            chat_id=cfg.ADMIN_CHAT_ID,
            text=f"Сбой публикации в Telegram для поста ID {post_id} после 3 попыток.\nПричина: {error_msg}",
        )


async def main():
    await ensure_group(cfg.STREAM_PUB_TG, GROUP_NAME)
    print("Воркер публикации TG запущен...")

    while True:
        try:
            entries = await redis.xreadgroup(
                GROUP_NAME,
                CONSUMER_NAME,
                {cfg.STREAM_PUB_TG: ">"},
                count=1,
                block=5000,
            )
            if not entries:
                continue

            for _, messages in entries:
                for msg_id, fields in messages:
                    payload = json.loads(fields["data"])
                    try:
                        await publish_to_tg(payload["post_id"])
                    except TelegramRetryAfter as e:
                        await asyncio.sleep(e.retry_after)
                        await handle_failure(payload, f"FloodWait {e.retry_after}s")
                    except Exception as e:
                        await handle_failure(payload, str(e))
                    finally:
                        await redis.xack(cfg.STREAM_PUB_TG, GROUP_NAME, msg_id)
        except Exception as e:
            print(f"Ошибка цикла TG паблишера: {e}")
            await asyncio.sleep(2)


if __name__ == "__main__":
    asyncio.run(main())