from datetime import datetime
from typing import List, Optional
from sqlalchemy import BigInteger, ForeignKey, String, Text, DateTime, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Post(Base):
    __tablename__ = "posts"

    id: Mapped[int] = mapped_column(primary_key=True)
    source_chat: Mapped[str] = mapped_column(String(128))
    orig_text: Mapped[str] = mapped_column(Text)
    ai_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    tags: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    simhash: Mapped[int] = mapped_column(BigInteger, index=True)
    # статусы: draft, queued, published, rejected, error
    status: Mapped[str] = mapped_column(String(32), default="draft", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    media: Mapped[List["Media"]] = relationship(
        back_populates="post", cascade="all, delete-orphan", lazy="selectin"
    )

    def formatted_text(self) -> str:
        base = self.ai_text or self.orig_text
        if self.tags:
            return f"{base}\n\n{self.tags}"
        return base


class Media(Base):
    __tablename__ = "media"

    id: Mapped[int] = mapped_column(primary_key=True)
    post_id: Mapped[int] = mapped_column(ForeignKey("posts.id", ondelete="CASCADE"))
    media_type: Mapped[str] = mapped_column(String(16))  # photo или video
    file_path: Mapped[str] = mapped_column(String(512))

    # кэшированные id площадок, чтобы не гонять файлы по сети повторно
    tg_file_id: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    vk_attach_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)

    post: Mapped["Post"] = relationship(back_populates="media")