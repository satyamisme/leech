from bot import LOGGER
from bot.core.config_manager import config_dict
from bot.helper.mirror_leech_utils.status_utils import get_readable_file_size
from bot.helper.mirror_leech_utils.gdrive_utils.clone import GoogleDriveClone
from bot.helper.telegram_helper.message_utils import send_message, delete_message, send_status_message
from bot.helper.telegram_helper.button_build import ButtonMaker
from bot.helper.ext_utils.bot_utils import new_task
from bot.helper.listeners.task_listener import TaskListener

@new_task
async def clone_node(client, message):
    listener = TaskListener()
    # ... rest of logic
