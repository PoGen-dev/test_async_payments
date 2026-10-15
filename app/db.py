from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.config import get_settings


def make_engine(url: str | None = None):
    return create_async_engine(url or get_settings().database_url, pool_pre_ping=True)


def make_session_factory(engine):
    return async_sessionmaker(engine, expire_on_commit=False)
