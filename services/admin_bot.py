import asyncio
import json
import os
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from core.config import cfg
from core.db import async_session, init_db
from core.models import Post
from core.redis_client import redis

router = Router()


class EditState(StatesGroup):
    waiting_new_text = State()


def mod_kb(post_id: int) -> InlineKeyboardMarkup:
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


@router.callback_query(F.data.startswith("pub:"))
async def approve_post(call: CallbackQuery):
    post_id = int(call.data.split(":")[1])

    async with async_session() as session:
        post = await session.get(Post, post_id)
        if not post or post.status != "draft":
            await call.answer("Запись уже обработана или не найдена.")
            return

        post.status = "queued"
        await session.commit()

    task_data = json.dumps({"post_id": post_id, "attempt": 1})
    await redis.xadd(cfg.STREAM_PUB_TG, {"data": task_data})
    await redis.xadd(cfg.STREAM_PUB_VK, {"data": task_data})

    await call.message.edit_text(
        f"{call.message.text}\n\n[Статус: Отправлено в очередь на публикацию]"
    )
    await call.answer("Пост передан воркерам публикации.")


@router.callback_query(F.data.startswith("drop:"))
async def reject_post(call: CallbackQuery):
    post_id = int(call.data.split(":")[1])

    async with async_session() as session:
        post = await session.get(Post, post_id)
        if not post:
            await call.answer("Запись не найдена.")
            return

        post.status = "rejected"
        # чистим локальные файлы, чтобы не забивать диск отклоненными постами
        for m in post.media:
            if os.path.exists(m.file_path):
                try:
                    os.remove(m.file_path)
                except OSError:
                    pass
        await session.commit()

    await call.message.edit_text(f"Материал ID {post_id} отклонен и удален из очереди.")
    await call.answer("Черновик сброшен.")


@router.callback_query(F.data.startswith("edit:"))
async def start_edit_post(call: CallbackQuery, state: FSMContext):
    post_id = int(call.data.split(":")[1])
    await state.set_state(EditState.waiting_new_text)
    await state.update_data(post_id=post_id, orig_msg_id=call.message.message_id)

    await call.message.answer(
        f"Отправь исправленный текст для поста ID {post_id} следующим сообщением. "
        f"Для отмены напиши слово: отмена"
    )
    await call.answer()


@router.message(EditState.waiting_new_text)
async def save_edited_text(message: Message, state: FSMContext, bot: Bot):
    if message.text and message.text.strip().lower() == "отмена":
        await state.clear()
        await message.answer("Редактирование отменено.")
        return

    data = await state.get_data()
    post_id = data["post_id"]
    orig_msg_id = data["orig_msg_id"]

    async with async_session() as session:
        post = await session.get(Post, post_id)
        if not post:
            await state.clear()
            await message.answer("Пост не найден в базе.")
            return

        post.ai_text = message.text.strip()
        await session.commit()
        updated_preview = (
            f"Материал обновлен (ID {post.id})\n"
            f"Источник: {post.source_chat}\n"
            f"Вложений: {len(post.media)}\n\n"
            f"{post.formatted_text()}"
        )

    await state.clear()
    try:
        await bot.edit_message_text(
            chat_id=message.chat.id,
            message_id=orig_msg_id,
            text=updated_preview[:4090],
            reply_markup=mod_kb(post_id),
        )
    except Exception:
        await message.answer(updated_preview[:4090], reply_markup=mod_kb(post_id))

    await message.answer("Текст перезаписан. Можно отправлять в релиз.")


async def main():
    await init_db()
    bot = Bot(token=cfg.TG_BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    print("Админ-бот запущен...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())