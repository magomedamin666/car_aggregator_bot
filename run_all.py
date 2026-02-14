import asyncio
import logging
import platform
import sys

from aiogram import Bot, Dispatcher

from app.bot.handlers import router
from app.core.config import settings
from app.parsers.berkat_parser import berkat_parse_task_async


if platform.system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def periodic_parsing() -> None:
    while True:
        logger.info("⏰ Starting berkat.ru parsing...")
        try:
            await berkat_parse_task_async()
            logger.info("✅ Parsing completed. Next run in 10 minutes.")
        except Exception as e:
            logger.error(f"❌ Parsing error: {e}")

        await asyncio.sleep(600)


async def main() -> None:
    bot = Bot(token=settings.BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)

    asyncio.create_task(periodic_parsing())

    logger.info("=" * 60)
    logger.info("✅ CarBot started!")
    logger.info("   • Bot is accepting commands")
    logger.info("   • Parsing berkat.ru every 10 minutes")
    logger.info("   • Duplicate-free notifications")
    logger.info("=" * 60)

    try:
        await dp.start_polling(bot)
    except KeyboardInterrupt:
        logger.info("👋 Bot stopped by user")
    finally:
        await bot.session.close()
        logger.info("✅ System shut down correctly")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 System stopped by user")
    except Exception as e:
        logger.exception(f"❌ Critical error: {e}")
        sys.exit(1)