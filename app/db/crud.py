import logging
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import func

from app.db.models import Ad, FilterSet, SentNotification, SubscriptionStatus, User


logger = logging.getLogger(__name__)


async def get_user_by_telegram_id(db: AsyncSession, telegram_id: int) -> Optional[User]:
    """Получает пользователя по его Telegram ID."""
    stmt = select(User).where(User.telegram_id == telegram_id)
    result = await db.execute(stmt)
    return result.scalar_one_or_none()


async def create_user(
    db: AsyncSession,
    telegram_id: int,
    username: str,
    subscription_status: str = "trial",
) -> User:
    """Создаёт нового пользователя в системе."""
    user = User(
        telegram_id=telegram_id,
        username=username,
        subscription_status=subscription_status,
        created_at=func.now(),
    )
    db.add(user)
    try:
        await db.commit()
        await db.refresh(user)
    except Exception:
        await db.rollback()
        raise
    return user


async def create_filter_set(
    db: AsyncSession,
    user_id: int,
    name: str,
    filters_json: dict,
) -> FilterSet:
    """Создаёт новый фильтр для пользователя."""
    fs = FilterSet(
        user_id=user_id,
        name=name,
        filters_json=filters_json,
        is_active=True,
    )
    db.add(fs)
    try:
        await db.commit()
        await db.refresh(fs)
    except Exception:
        await db.rollback()
        raise
    return fs


async def get_active_filters(db: AsyncSession, user_id: int) -> List[FilterSet]:
    """Получает все активные фильтры пользователя."""
    result = await db.execute(
        select(FilterSet).where(
            and_(FilterSet.user_id == user_id, FilterSet.is_active == True)
        )
    )
    return result.scalars().all()


async def get_all_active_filters(db: AsyncSession) -> List[FilterSet]:
    """Получает все активные фильтры всех пользователей."""
    result = await db.execute(select(FilterSet).where(FilterSet.is_active == True))
    return result.scalars().all()


async def update_filter_set(
    db: AsyncSession,
    filter_id: int,
    name: Optional[str] = None,
    filters_json: Optional[dict] = None,
    is_active: Optional[bool] = None,
) -> Optional[FilterSet]:
    """Обновляет параметры фильтра."""
    fs = await db.get(FilterSet, filter_id)
    if not fs:
        return None

    if name is not None:
        fs.name = name
    if filters_json is not None:
        fs.filters_json = filters_json
    if is_active is not None:
        fs.is_active = is_active

    try:
        await db.commit()
        await db.refresh(fs)
    except Exception:
        await db.rollback()
        raise
    return fs


async def get_ad_by_source_external(
    db: AsyncSession, source: str, external_id: str
) -> Optional[Ad]:
    """Получает объявление по источнику и внешнему ID."""
    stmt = select(Ad).where(
        and_(Ad.source == source, Ad.external_id == external_id)
    )
    result = await db.execute(stmt)
    return result.scalar_one_or_none()


async def create_ad(db: AsyncSession, ad_data: dict) -> Ad:
    """Создаёт новое объявление в базе данных."""
    ad = Ad(**ad_data)
    db.add(ad)
    try:
        await db.commit()
        await db.refresh(ad)
    except Exception:
        await db.rollback()
        raise
    return ad


async def get_new_ads(db: AsyncSession, since: Optional[datetime] = None) -> List[Ad]:
    """Получает объявления, спарсенные после указанного времени."""
    if since is None:
        since = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=1)

    stmt = select(Ad).where(Ad.parsed_at >= since).order_by(Ad.parsed_at.desc())
    result = await db.execute(stmt)
    return result.scalars().all()


async def has_sent_notification(
    db: AsyncSession, user_id: int, ad_id: int, filter_id: int
) -> bool:
    """Проверяет, отправлялось ли уже уведомление пользователю по этому объявлению и фильтру."""
    stmt = select(SentNotification).where(
        and_(
            SentNotification.user_id == user_id,
            SentNotification.ad_id == ad_id,
            SentNotification.filter_id == filter_id,
        )
    )
    result = await db.execute(stmt)
    return result.scalar_one_or_none() is not None


async def mark_notification_sent(
    db: AsyncSession, user_id: int, ad_id: int, filter_id: int
) -> None:
    """Отмечает уведомление как отправленное."""
    notification = SentNotification(
        user_id=user_id,
        ad_id=ad_id,
        filter_id=filter_id,
    )
    db.add(notification)
    try:
        await db.commit()
    except Exception as e:
        await db.rollback()
        logger.warning(f"Не удалось отметить уведомление как отправленное: {e}")