from os import path as ospath
from os.path import basename, dirname
from bot import LOGGER
from bot.helper.ext_utils.files_utils import get_path_size, is_video, clean_target, get_mime_type, clean_download
from bot.helper.ext_utils.bot_utils import get_readable_file_size, get_readable_time, SetInterval, sync_to_async
from bot.helper.mirror_leech_utils.status_utils.upload_status import UploadStatus
from bot.helper.mirror_leech_utils.telegram_uploader import TelegramUploader
from bot.helper.video_utils.processor import process_video
from ..common import TaskConfig
from ...core.config_manager import config_dict, Config
from bot.helper.telegram_helper.message_utils import send_message, delete_message, edit_message
from bot.helper.telegram_helper.button_build import ButtonMaker
from html import escape
from asyncio import sleep, gather, Event
from aiofiles.os import path as aiopath, remove
from ..ext_utils.db_handler import database
from ... import task_dict_lock, task_dict, queue_dict_lock, non_queued_up, non_queued_dl, queued_dl, queued_up, same_directory_lock, DOWNLOAD_DIR, intervals
from ...core.torrent_manager import TorrentManager
from ..ext_utils.task_manager import start_from_queued, check_running_tasks
from ..mirror_leech_utils.gdrive_utils.upload import GoogleDriveUpload
from ..mirror_leech_utils.rclone_utils.transfer import RcloneTransferHelper
from ..mirror_leech_utils.status_utils.gdrive_status import GoogleDriveStatus
from ..mirror_leech_utils.status_utils.queue_status import QueueStatus
from ..mirror_leech_utils.status_utils.rclone_status import RcloneStatus
from ..mirror_leech_utils.status_utils.telegram_status import TelegramStatus
from time import time
from pyrogram.errors import RPCError
from ..ext_utils.status_utils import get_progress_bar_string


class TaskListener(TaskConfig):
    def __init__(self):
        super().__init__()
        self.streams_kept = None
        self.streams_removed = None
        self.media_info = {}
        self.status_message = None
        self.start_time = time()
        self.last_progress_text = None
        self.total_parts = 1
        self.current_part = 1

    async def on_task_created(self):
        self.status_message = await send_message(self.message, "🎬 Analyzing Streams... ⏳")

    async def clean(self):
        try:
            if st := intervals["status"]:
                for intvl in list(st.values()):
                    intvl.cancel()
                intervals["status"].clear()
            await gather(TorrentManager.aria2.purgeDownloadResult(), delete_message(self.status_message))
        except:
            pass

    def clear(self):
        self.subname = ""
        self.subsize = 0
        self.files_to_proceed = []
        self.proceed_count = 0
        self.progress = True

    async def remove_from_same_dir(self):
        async with task_dict_lock:
            if (self.folder_name and self.same_dir and self.mid in self.same_dir[self.folder_name]["tasks"]):
                self.same_dir[self.folder_name]["tasks"].remove(self.mid)
                self.same_dir[self.folder_name]["total"] -= 1

    async def on_download_start(self):
        if (self.is_super_chat and Config.INCOMPLETE_TASK_NOTIFIER and Config.DATABASE_URL):
            await database.add_incomplete_task(self.message.chat.id, self.message.link, self.tag)

    async def on_download_complete(self):
        if self.is_cancelled:
            return

        up_path = f"{self.dir}/{self.name}"

        if await aiopath.isdir(up_path):
            video_files = []
            async for root, _, files in aiopath.walk(up_path):
                for f in files:
                    fp = ospath.join(root, f)
                    if (await get_mime_type(fp)).startswith("video") and not f.startswith('.'):
                        video_files.append(fp)
            if not video_files:
                await self.on_upload_error("No video files found in the downloaded folder.")
                return

            for video_file in video_files:
                self.name = basename(video_file)
                await self._process_and_upload(video_file)
                if self.is_cancelled:
                    return
        else:
            await self._process_and_upload(up_path)

        await clean_download(self.dir)
        if hasattr(self, 'up_dir') and self.up_dir and await aiopath.exists(self.up_dir):
            await clean_download(self.up_dir)
        if self.thumb and await aiopath.exists(self.thumb):
            await remove(self.thumb)

    async def _process_and_upload(self, up_path):
        """Process video (if leech) and upload."""
        if self.is_leech and not self.compress:
            processed_path, self.media_info = await process_video(up_path, self)
            if not processed_path or self.is_cancelled:
                return
            upload_path = processed_path
        else:
            upload_path = up_path

        tg_uploader = TelegramUploader(self, upload_path)
        async for sent_message in tg_uploader.upload():
            if self.is_cancelled:
                return
            if sent_message:
                await self._send_leech_completion_message(sent_message)

    async def _send_leech_completion_message(self, sent_message):
        """Send rich completion message with real stream info."""
        if not sent_message:
            return

        msg = ""

        if sent_message.document:
            name = sent_message.document.file_name
            size = sent_message.document.file_size
        elif sent_message.video:
            name = sent_message.video.file_name or self.name
            size = sent_message.video.file_size
        else:
            name = self.name
            size = self.size

        msg += f"🎬 <code>{escape(name)}</code>\n"
        msg += f"📁 Part {self.current_part} of {self.total_parts} | 📂 Total: {get_readable_file_size(size)} | ⏱️ {get_readable_time(time() - self.start_time)}\n\n"

        if self.media_info:
            resolution = f"{self.media_info.get('height', 'N/A')}p"
            codec = self.media_info.get('video_codec', 'N/A').upper()
            audio_lang = self.media_info.get('audio_lang', 'N/A').upper()
            msg += f"📊 {resolution} • {codec} • {audio_lang} • Split\n"
        else:
            msg += f"📊 File Upload\n"

        msg += f"📡 Source: {self.tag}\n\n"
        msg += f"📽️ <code>{escape(name)}</code>\n"
        msg += f"📏 {get_readable_file_size(size)} | 📅 {get_readable_time(time())}\n\n"

        if self.streams_kept:
            msg += "**Streams Kept:**\n"
            for s in self.streams_kept:
                msg += f"🎥 {s}\n"

        if self.streams_removed:
            msg += "**Streams Removed:**\n"
            for s in self.streams_removed:
                msg += f"🚫 {s}\n"

        msg += f"\n✅ Upload Complete (Part {self.current_part}/{self.total_parts})"
        if self.current_part == self.total_parts:
            msg += "\n✨ All parts uploaded successfully!"
            msg += f"\n🔗 Files are now available in your chat."
            msg += f"\n⚡️ {self.tag}"

        buttons = ButtonMaker()
        if hasattr(sent_message, 'link') and sent_message.link:
            buttons.url_button("Download Link", sent_message.link)
        reply_markup = buttons.build_menu(2) if buttons._button else None

        try:
            await send_message(self.message, msg, reply_markup)
        except Exception as e:
            LOGGER.error(f"Failed to send completion message: {e}")
            if "BUTTON_URL_INVALID" in str(e):
                await send_message(self.message, msg)

    async def on_upload_complete(self, link, files, folders, mime_type, rclone_path="", dir_id="", tg_sent_messages=None):
        if self.is_super_chat and Config.INCOMPLETE_TASK_NOTIFIER and Config.DATABASE_URL:
            await database.rm_complete_task(self.message.link)

        msg = f"🎉 <b>Task Completed by {self.tag}</b>\n"
        msg += f"<b>Name:</b> <code>{self.name}</code>\n"
        msg += f"<b>Size:</b> {get_readable_file_size(self.size)}\n"
        msg += f"<b>cc:</b> {self.tag}"

        buttons = ButtonMaker()
        if link:
            buttons.url_button("Cloud Link", link)
        reply_markup = buttons.build_menu(2) if buttons._button else None

        await send_message(self.message, msg, reply_markup)

        if self.seed:
            await clean_target(self.up_dir)
        else:
            await clean_download(self.dir)

        async with task_dict_lock:
            if self.mid in task_dict:
                del task_dict[self.mid]
            count = len(task_dict)
        if count == 0:
            await self.clean()
        else:
            await update_status_message(self.message.chat.id)

        async with queue_dict_lock:
            if self.mid in non_queued_up:
                non_queued_up.remove(self.mid)

        await start_from_queued()

    async def on_upload_error(self, error):
        await send_message(self.message, f"❌ {escape(str(error))}")
        self.is_cancelled = True
        async with task_dict_lock:
            if self.mid in task_dict:
                del task_dict[self.mid]
            count = len(task_dict)
            if count == 0:
                await self.clean()
            else:
                await update_status_message(self.message.chat.id)
        if self.is_super_chat and Config.INCOMPLETE_TASK_NOTIFIER and Config.DATABASE_URL:
            await database.rm_complete_task(self.message.link)
