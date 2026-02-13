import asyncio
import logging
import platform
import sys

from aiogram import Bot, Dispatcher

from app.bot.handlers import router
from app.core.config import settings
from app.db.session import async_session, target_metadata
from app.parsers.berkat_parser import berkat_parse_task_async


if platform.system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def create_tables():
    """Создаёт таблицы в БД если их нет"""
    async with async_session() as db:
        # Создаём все таблицы, указанные в metadata
        from app.db.models import Base
        async with db.bind.connect() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("✅ Таблицы в БД созданы/проверены")


async def periodic_parsing():
    """Парсинг каждые 10 минут"""
    while True:
        logger.info("⏰ Запуск парсинга berkat.ru...")
        try:
            await berkat_parse_task_async()
            logger.info("✅ Парсинг завершён. Следующий запуск через 10 минут.")
        except Exception as e:
            logger.error(f"❌ Ошибка парсинга: {e}")
        
        await asyncio.sleep(600)


async def main():
    # Создаём таблицы при запуске
    await create_tables()

    bot = Bot(token=settings.BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    
    # Запускаем парсер в фоне
    asyncio.create_task(periodic_parsing())
    
    logger.info("=" * 60)
    logger.info("✅ CarBot запущен!")
    logger.info("   • Бот принимает команды")
    logger.info("   • Парсинг berkat.ru каждые 10 минут")
    logger.info("   • Уведомления без дубликатов")
    logger.info("=" * 60)
    
    try:
        await dp.start_polling(bot)
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен пользователем")
    finally:
        await bot.session.close()
        logger.info("✅ Система остановлена корректно")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Система остановлена пользователем")
    except Exception as e:
        logger.exception(f"❌ Критическая ошибка: {e}")
        sys.exit(1)