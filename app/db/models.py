from enum import Enum as PyEnum

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Enum as SQLAlchemyEnum,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship


Base = declarative_base()


class SubscriptionStatus(PyEnum):
    trial = "trial"
    active = "active"
    expired = "expired"


class User(Base):
    __tablename__ = "users"

    telegram_id = Column(BigInteger, primary_key=True, index=True)
    username = Column(String(255), nullable=True)
    subscription_status = Column(
        SQLAlchemyEnum(SubscriptionStatus),
        default=SubscriptionStatus.trial,
        nullable=False,
    )
    subscription_end = Column(DateTime, nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)

    filter_sets = relationship(
        "FilterSet", back_populates="user", cascade="all, delete-orphan"
    )


class FilterSet(Base):
    __tablename__ = "filter_sets"

    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, ForeignKey("users.telegram_id"), nullable=False)
    name = Column(String(100), nullable=False)
    filters_json = Column(JSONB, nullable=False)
    is_active = Column(Boolean, default=True, server_default="true", nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())

    user = relationship("User", back_populates="filter_sets")


class Ad(Base):
    __tablename__ = "ads"

    id = Column(Integer, primary_key=True)
    source = Column(String(50), nullable=False, index=True)
    external_id = Column(String(100), nullable=False)
    title = Column(String(255), nullable=False)
    price = Column(Integer, nullable=True, index=True)
    brand = Column(String(100), nullable=True, index=True)
    model = Column(String(100), nullable=True, index=True)
    year = Column(Integer, nullable=True, index=True)
    mileage = Column(Integer, nullable=True, index=True)
    region = Column(String(100), nullable=True, index=True)
    url = Column(String(512), nullable=False)
    photo_url = Column(String(512), nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    parsed_at = Column(DateTime, server_default=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint("source", "external_id", name="unique_ad_source_external"),
        Index("ix_ads_brand_model_year", "brand", "model", "year"),
        Index("ix_ads_price_region", "price", "region"),
    )


class SentNotification(Base):
    __tablename__ = "sent_notifications"

    id = Column(Integer, primary_key=True)
    user_id = Column(
        BigInteger,
        ForeignKey("users.telegram_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    ad_id = Column(
        Integer,
        ForeignKey("ads.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    filter_id = Column(
        Integer,
        ForeignKey("filter_sets.id", ondelete="CASCADE"),
        nullable=False,
    )
    sent_at = Column(DateTime, server_default=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "user_id", "ad_id", "filter_id", name="unique_user_ad_filter"
        ),
    )