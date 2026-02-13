import logging

from aiogram import Bot

from app.core.config import settings


logger = logging.getLogger(__name__)
bot = Bot(token=settings.BOT_TOKEN)


async def send_ad_notification(telegram_id: int, ad, filter_name: str) -> None:
    """Отправляет уведомление о новом объявлении пользователю."""
    try:
        message = f"🚗 <b>Новое объявление по вашему фильтру: {filter_name}</b>\n\n"

        if ad.brand and ad.model:
            message += f"🔹 <b>{ad.brand} {ad.model}</b>\n"
        elif ad.title:
            message += f"🔹 <b>{ad.title}</b>\n"

        if ad.year:
            message += f"📅 Год: {ad.year}\n"

        if ad.price:
            price_str = f"{ad.price:,}".replace(",", " ")
            message += f"💰 Цена: {price_str} ₽\n"

        if ad.mileage:
            mileage_str = f"{ad.mileage:,}".replace(",", " ")
            message += f"🛣️ Пробег: {mileage_str} км\n"

        if ad.region:
            message += f"📍 Регион: {ad.region}\n"

        message += f"\n🔗 <a href='{ad.url}'>Посмотреть объявление</a>"

        if ad.photo_url:
            try:
                await bot.send_photo(
                    chat_id=telegram_id,
                    photo=ad.photo_url,
                    caption=message,
                    parse_mode="HTML",
                )
            except Exception as photo_error:
                logger.warning(f"Ошибка отправки фото: {photo_error}. Отправляем без фото.")
                await bot.send_message(
                    chat_id=telegram_id,
                    text=message,
                    parse_mode="HTML",
                )
        else:
            await bot.send_message(
                chat_id=telegram_id,
                text=message,
                parse_mode="HTML",
            )

        logger.info(f"Уведомление отправлено пользователю {telegram_id}")

    except Exception as e:
        logger.error(f"Ошибка отправки уведомления пользователю {telegram_id}: {e}")
        raise