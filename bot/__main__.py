import asyncio
import signal
from bot import bot_loop
from bot.core.config_manager import config_dict
from bot.helper.telegram_helper.bot_client import Bot
from bot.modules import (
    __help__,
    __modules__,
)
from bot.helper.telegram_helper.message_utils import auto_delete_message
from bot.helper.ext_utils.telegraph_helper import telegraph
from bot.helper.ext_utils.db_handler import DbManger
from logging import getLogger

LOGGER = getLogger(__name__)

async def main():
    # Load config first
    if not config_dict:
        LOGGER.error("Config not loaded. Exiting.")
        exit(1)

    # Initialize bot client
    bot = await Bot().start()

    # Import modules after config is ready
    from .modules import (
        __help__,
        __modules__,
    )

    # Start database if enabled
    if config_dict.get('DATABASE_URL'):
        await DbManger().initiate_db()

    # Start bot
    try:
        await bot.start()
        LOGGER.info("Bot started!")
        await asyncio.Event().wait()  # Keep alive
    except KeyboardInterrupt:
        await bot.stop()
    finally:
        if hasattr(bot, 'client') and bot.client:
            await bot.client.stop()

if __name__ == "__main__":
    bot_loop.run_until_complete(main())
