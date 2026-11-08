import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    DB_URL = os.getenv("DB_URL", "")
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    TG_API_ID = int(os.getenv("TG_API_ID", "0"))
    TG_API_HASH = os.getenv("TG_API_HASH", "")
    TG_SESSION_STRING = os.getenv("TG_SESSION_STRING", "")

    raw_channels = os.getenv("SOURCE_CHANNELS", "")
    SOURCE_CHANNELS = [
        int(ch.strip()) if ch.strip().lstrip("-").isdigit() else ch.strip()
        for ch in raw_channels.split(",")
        if ch.strip()
    ]

    TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
    ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))
    TARGET_TG_CHANNEL_ID = int(os.getenv("TARGET_TG_CHANNEL_ID", "0"))

    VK_GROUP_TOKEN = os.getenv("VK_GROUP_TOKEN", "")
    VK_GROUP_ID = int(os.getenv("VK_GROUP_ID", "0"))

    LLM_BASE_URL = os.getenv("LLM_BASE_URL", "https://api.openai.com/v1").rstrip("/")
    LLM_API_KEY = os.getenv("LLM_API_KEY", "")
    LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")

    MEDIA_DIR = "/app/media_storage"

    # названия стримов в redis
    STREAM_RAW = "stream:raw_posts"
    STREAM_PUB_TG = "stream:pub_tg"
    STREAM_PUB_VK = "stream:pub_vk"
    STREAM_DLQ = "stream:dlq"


cfg = Config()