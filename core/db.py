from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from core.config import cfg
from core.models import Base

engine = create_async_engine(cfg.DB_URL, echo=False, pool_size=10, max_overflow=20)
async_session = async_sessionmaker(engine, expire_on_commit=False)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)