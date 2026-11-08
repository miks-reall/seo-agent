import asyncio
import json
import os
import uuid
from pyrogram import Client, filters
from pyrogram.types import Message
from core.config import cfg
from core.redis_client import redis

app = Client(
    "collector_session",
    api_id=cfg.TG_API_ID,
    api_hash=cfg.TG_API_HASH,
    session_string=cfg.TG_SESSION_STRING,
)

# буфер для сборки альбомов по media_group_id
album_buffer: dict[str, list[Message]] = {}
album_tasks: dict[str, asyncio.Task] = {}


async def download_msg_media(msg: Message) -> dict | None:
    m_type = None
    ext = ""
    if msg.photo:
        m_type = "photo"
        ext = "jpg"
    elif msg.video:
        m_type = "video"
        ext = "mp4"
    else:
        return None

    filename = f"{uuid.uuid4().hex}.{ext}"
    filepath = os.path.join(cfg.MEDIA_DIR, filename)
    await msg.download(file_name=filepath)
    return {"type": m_type, "path": filepath}


async def flush_album(group_id: str):
    await asyncio.sleep(2.0)
    messages = album_buffer.pop(group_id, [])
    album_tasks.pop(group_id, None)
    if not messages:
        return

    messages.sort(key=lambda m: m.id)
    text = ""
    media_list = []

    for m in messages:
        if not text and (m.text or m.caption):
            text = m.text or m.caption
        media_item = await download_msg_media(m)
        if media_item:
            media_list.append(media_item)

    if not text:
        return

    payload = {
        "source": messages[0].chat.username or str(messages[0].chat.id),
        "text": text,
        "media": media_list,
    }
    await redis.xadd(cfg.STREAM_RAW, {"data": json.dumps(payload, ensure_ascii=False)})


@app.on_message(filters.chat(cfg.SOURCE_CHANNELS))
async def handle_channel_post(client: Client, message: Message):
    if message.media_group_id:
        gid = str(message.media_group_id)
        album_buffer.setdefault(gid, []).append(message)
        if gid not in album_tasks:
            album_tasks[gid] = asyncio.create_task(flush_album(gid))
        return

    text = message.text or message.caption or ""
    if not text:
        return

    media_list = []
    media_item = await download_msg_media(message)
    if media_item:
        media_list.append(media_item)

    payload = {
        "source": message.chat.username or str(message.chat.id),
        "text": text,
        "media": media_list,
    }
    await redis.xadd(cfg.STREAM_RAW, {"data": json.dumps(payload, ensure_ascii=False)})


if __name__ == "__main__":
    print("Запуск парсера каналов...")
    app.run()