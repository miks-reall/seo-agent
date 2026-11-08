import asyncio
import time
from redis.asyncio import Redis
from redis.exceptions import ResponseError
from core.config import cfg

redis = Redis.from_url(cfg.REDIS_URL, decode_responses=True)

# lua-скрипт для атомарного token bucket в redis
TOKEN_BUCKET_LUA = """
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local data = redis.call('HMGET', key, 'tokens', 'last_updated')
local tokens = tonumber(data[1])
local last_updated = tonumber(data[2])

if tokens == nil then
    tokens = capacity
    last_updated = now
else
    local delta = math.max(0, now - last_updated)
    tokens = math.min(capacity, tokens + delta * rate)
    last_updated = now
end

if tokens >= 1.0 then
    tokens = tokens - 1.0
    redis.call('HMSET', key, 'tokens', tokens, 'last_updated', last_updated)
    redis.call('EXPIRE', key, 3600)
    return 0
else
    local wait_time = (1.0 - tokens) / rate
    return wait_time
end
"""


async def ensure_group(stream: str, group: str):
    try:
        await redis.xgroup_create(stream, group, id="0", mkstream=True)
    except ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


async def acquire_token(bucket_name: str, rate: float, capacity: int):
    key = f"limiter:{bucket_name}"
    while True:
        now = time.time()
        wait_time = await redis.eval(TOKEN_BUCKET_LUA, 1, key, rate, capacity, now)
        wait_sec = float(wait_time)
        if wait_sec <= 0:
            return
        await asyncio.sleep(wait_sec)