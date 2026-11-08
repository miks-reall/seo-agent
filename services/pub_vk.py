import asyncio
import json
import aiohttp
from vkbottle import API
from core.config import cfg
from core.db import async_session
from core.models import Post, Media
from core.redis_client import redis, ensure_group, acquire_token

GROUP_NAME = "vk_pub_group"
CONSUMER_NAME = "vk_worker_1"

vk_api = API(token=cfg.VK_GROUP_TOKEN)


async def upload_vk_media(m: Media) -> str | None:
    # кэш уже есть в базе — отдаем сразу без повторной загрузки файла
    if m.vk_attach_id:
        return m.vk_attach_id

    await acquire_token("vk_api_limit", rate=2.0, capacity=3)

    if m.media_type == "photo":
        server = await vk_api.photos.get_wall_upload_server(group_id=cfg.VK_GROUP_ID)
        async with aiohttp.ClientSession() as http:
            with open(m.file_path, "rb") as f:
                form = aiohttp.FormData()
                form.add_field("photo", f, filename="photo.jpg")
                async with http.post(server.upload_url, data=form) as resp:
                    up_res = await resp.json(content_type=None)

        await acquire_token("vk_api_limit", rate=2.0, capacity=3)
        saved = await vk_api.photos.save_wall_photo(
            group_id=cfg.VK_GROUP_ID,
            photo=up_res["photo"],
            server=up_res["server"],
            hash=up_res["hash"],
        )
        photo = saved[0]
        m.vk_attach_id = f"photo{photo.owner_id}_{photo.id}"
        return m.vk_attach_id

    elif m.media_type == "video":
        video_res = await vk_api.video.save(group_id=cfg.VK_GROUP_ID, wallpost=0)
        async with aiohttp.ClientSession() as http:
            with open(m.file_path, "rb") as f:
                form = aiohttp.FormData()
                form.add_field("video_file", f, filename="video.mp4")
                async with http.post(video_res.upload_url, data=form) as resp:
                    await resp.read()

        m.vk_attach_id = f"video{video_res.owner_id}_{video_res.video_id}"
        return m.vk_attach_id

    return None


async def publish_to_vk(post_id: int):
    async with async_session() as session:
        post = await session.get(Post, post_id)
        if not post:
            return

        attachments = []
        for m in post.media:
            att = await upload_vk_media(m)
            if att:
                attachments.append(att)

        await acquire_token("vk_api_limit", rate=2.0, capacity=3)
        await vk_api.wall.post(
            owner_id=-cfg.VK_GROUP_ID,
            from_group=1,
            message=post.formatted_text(),
            attachments=",".join(attachments) if attachments else None,
        )

        post.status = "published"
        await session.commit()


async def handle_vk_failure(payload: dict, error_msg: str):
    attempt = payload.get("attempt", 1)
    post_id = payload["post_id"]

    if attempt < 3:
        await asyncio.sleep(5 * attempt)
        payload["attempt"] = attempt + 1
        await redis.xadd(cfg.STREAM_PUB_VK, {"data": json.dumps(payload)})
    else:
        await redis.xadd(
            cfg.STREAM_DLQ,
            {"platform": "vk", "post_id": str(post_id), "error": error_msg},
        )


async def main():
    await ensure_group(cfg.STREAM_PUB_VK, GROUP_NAME)
    print("Воркер публикации VK запущен...")

    while True:
        try:
            entries = await redis.xreadgroup(
                GROUP_NAME,
                CONSUMER_NAME,
                {cfg.STREAM_PUB_VK: ">"},
                count=1,
                block=5000,
            )
            if not entries:
                continue

            for _, messages in entries:
                for msg_id, fields in messages:
                    payload = json.loads(fields["data"])
                    try:
                        await publish_to_vk(payload["post_id"])
                    except Exception as e:
                        print(f"Ошибка публикации в ВК: {e}")
                        await handle_vk_failure(payload, str(e))
                    finally:
                        await redis.xack(cfg.STREAM_PUB_VK, GROUP_NAME, msg_id)
        except Exception as e:
            print(f"Ошибка цикла VK паблишера: {e}")
            await asyncio.sleep(2)


if __name__ == "__main__":
    asyncio.run(main())