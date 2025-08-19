from aiofiles.os import path as aiopath, listdir, remove
from asyncio import sleep, gather
from os import path as ospath
from html import escape
from requests import utils as rutils

from ... import (
    intervals,
    task_dict,
    task_dict_lock,
    LOGGER,
    non_queued_up,
    non_queued_dl,
    queued_up,
    queued_dl,
    queue_dict_lock,
    same_directory_lock,
    DOWNLOAD_DIR,
)
from ...core.config_manager import Config
from ...core.torrent_manager import TorrentManager
from ..common import TaskConfig
from ..ext_utils.bot_utils import sync_to_async
from ..ext_utils.db_handler import database
from ..ext_utils.files_utils import (
    get_path_size,
    clean_download,
    clean_target,
    join_files,
    create_recursive_symlink,
    remove_excluded_files,
    move_and_merge,
    is_video,
)
from ..ext_utils.links_utils import is_gdrive_id
from ..ext_utils.status_utils import get_readable_file_size, get_readable_time
from ..ext_utils.task_manager import start_from_queued, check_running_tasks
from ..mirror_leech_utils.gdrive_utils.upload import GoogleDriveUpload
from ..mirror_leech_utils.rclone_utils.transfer import RcloneTransferHelper
from ..mirror_leech_utils.status_utils.gdrive_status import GoogleDriveStatus
from ..mirror_leech_utils.status_utils.queue_status import QueueStatus
from ..mirror_leech_utils.status_utils.rclone_status import RcloneStatus
from ..mirror_leech_utils.status_utils.telegram_status import TelegramStatus
from ..mirror_leech_utils.telegram_uploader import TelegramUploader
from ..telegram_helper.button_build import ButtonMaker
from ..video_utils.processor import process_video
from ..telegram_helper.message_utils import (
    send_message,
    delete_status,
    update_status_message,
    send_status_message,
    edit_message,
    delete_message,
)
from ..ext_utils.bot_utils import SetInterval
from ..ext_utils.status_utils import get_progress_bar_string
from bot.helper.utilities.smart_split import smart_split_if_needed
import os
from time import time
from datetime import datetime

class TaskListener(TaskConfig):
    def __init__(self):
        super().__init__()
        self.streams_kept = None
        self.streams_removed = None
        self.media_info = None
        self.status_message = None
        self.start_time = time()
        self.last_ffmpeg_progress_text = None
        self.last_progress_time = 0
        self.last_progress_msg = ""
        self.kept_indices = set()
        self.kept_audio_langs = []
        self.removed_audio_langs = []
        self.removed_subtitle_langs = []


    async def on_task_created(self):
        self.status_message = await send_message(self.message, "🎬 Analyzing Streams... ⏳")

    async def clean(self):
        try:
            if st := intervals["status"]:
                for intvl in list(st.values()):
                    intvl.cancel()
            intervals["status"].clear()
            await gather(TorrentManager.aria2.purgeDownloadResult(), delete_status())
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
            if (
                self.folder_name
                and self.same_dir
                and self.mid in self.same_dir[self.folder_name]["tasks"]
            ):
                self.same_dir[self.folder_name]["tasks"].remove(self.mid)
                self.same_dir[self.folder_name]["total"] -= 1

    async def on_download_start(self):
        if (
            self.is_super_chat
            and Config.INCOMPLETE_TASK_NOTIFIER
            and Config.DATABASE_URL
        ):
            await database.add_incomplete_task(
                self.message.chat.id, self.message.link, self.tag
            )

    async def on_download_progress(self):
        if self.status_message is None:
            return
        now = time()
        if now - self.last_progress_time < 1:
            return
        self.last_progress_time = now
        async with task_dict_lock:
            if self.mid not in task_dict:
                return
            download = task_dict[self.mid]
            progress = download.progress()
            msg = f"**Downloading:** `{self.name}`\n{get_progress_bar_string(progress)} {progress:.2f}%"
            if hasattr(self, 'last_progress_msg') and self.last_progress_msg == msg:
                return
            try:
                await edit_message(self.status_message, msg)
                self.last_progress_msg = msg
            except:
                pass

    async def on_download_complete(self):
        await sleep(2)
        if self.is_cancelled:
            return
        multi_links = False
        if (
            self.folder_name
            and self.same_dir
            and self.mid in self.same_dir[self.folder_name]["tasks"]
        ):
            async with same_directory_lock:
                while True:
                    async with task_dict_lock:
                        if self.mid not in self.same_dir[self.folder_name]["tasks"]:
                            return
                        if (
                            self.same_dir[self.folder_name]["total"] <= 1
                            or len(self.same_dir[self.folder_name]["tasks"]) > 1
                        ):
                            if self.same_dir[self.folder_name]["total"] > 1:
                                self.same_dir[self.folder_name]["tasks"].remove(self.mid)
                                self.same_dir[self.folder_name]["total"] -= 1
                                spath = f"{self.dir}{self.folder_name}"
                                des_id = list(self.same_dir[self.folder_name]["tasks"])[0]
                                des_path = f"{DOWNLOAD_DIR}{des_id}{self.folder_name}"
                                LOGGER.info(f"Moving files from {self.mid} to {des_id}")
                                await move_and_merge(spath, des_path, self.mid)
                                multi_links = True
                            break
                    await sleep(1)

        async with task_dict_lock:
            if self.is_cancelled: return
            if self.mid not in task_dict: return
            download = task_dict[self.mid]
            self.name = download.name()
            gid = download.gid()
        LOGGER.info(f"Download completed: {self.name}")

        if multi_links:
            await self.on_upload_error(f"{self.name} Downloaded!\n\nWaiting for other tasks to finish...")
            return

        dl_path = f"{self.dir}/{self.name}"
        up_path = dl_path

        if self.extract:
            up_path = await self.proceed_extract(up_path, gid)
            if self.is_cancelled: return
            self.name = up_path.replace(f"{self.dir}/", "").split("/", 1)[0]

        if await is_video(up_path):
            if self.status_message:
                await edit_message(self.status_message, f"🎬 **Processing Video:** `{self.name}`\n\n⏳ This may take a while...")

            processed_path, self.media_info, self.kept_indices = await process_video(up_path, self)

            if self.is_cancelled: return
            if processed_path:
                up_path = processed_path
                self.name = up_path.replace(f"{self.dir}/", "").split("/", 1)[0]

            from time import strftime

            if self.status_message:
                await edit_message(self.status_message, f"🔪 **Splitting file:** `{self.name}`")
            split_files = await sync_to_async(smart_split_if_needed, up_path)

            if len(split_files) > 1:
                if self.status_message:
                    await edit_message(self.status_message, f"📤 **Uploading {len(split_files)} parts:** `{self.name}`")

                LOGGER.info(f"Splitting successful. Uploading {len(split_files)} parts.")
                upload_dir = ospath.dirname(split_files[0])
                tg_uploader = TelegramUploader(self, upload_dir)
                tg_uploader._sent_msg = self.status_message or self.message
                await tg_uploader._user_settings()

                sent_messages = []
                reply_to_message_id = None

                for file_path in split_files:
                    if self.is_cancelled:
                        LOGGER.info("Upload cancelled by user.")
                        break

                    tg_uploader._up_path = file_path
                    tg_uploader._reply_to = reply_to_message_id

                    async for sent_message in tg_uploader.upload():
                        if sent_message:
                            sent_messages.append(sent_message)
                            reply_to_message_id = sent_message.id
                        else:
                            LOGGER.error(f"Failed to upload part: {file_path}. Stopping upload process.")
                            await self.on_upload_error("A part failed to upload, stopping.")
                            return

                if self.is_cancelled:
                    await self.on_upload_error("Upload was cancelled.")
                    return

                if sent_messages:
                    await self._send_master_summary(sent_messages, split_files)

                if self.status_message:
                    await delete_message(self.status_message)
                return

        # Fallback for non-video files or single part videos
        if self.is_leech:
             await self.proceed_leech(up_path, gid)

    async def proceed_leech(self, up_path, gid):
        if self.status_message:
            await edit_message(self.status_message, f"🎬 **Uploading:** `{self.name}` 📤")
        LOGGER.info(f"Leech Name: {self.name}")
        upload_path = up_path
        if await aiopath.isfile(upload_path):
            upload_path = ospath.dirname(upload_path)
        tg = TelegramUploader(self, upload_path)
        async with task_dict_lock:
            task_dict[self.mid] = TelegramStatus(self, tg, gid, "up")

        async for sent_message in tg.upload():
            if self.is_cancelled:
                break
            if sent_message:
                await self._send_leech_completion_message(sent_message)

        if self.is_cancelled: return
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

    async def _send_master_summary(self, sent_messages, file_paths):
        if not file_paths:
            return

        base_name = ospath.splitext(ospath.basename(file_paths[0]))[0]
        if " - Part 001" in base_name:
            base_name = base_name.replace(" - Part 001", "")

        total_size_bytes = sum(os.path.getsize(f) for f in file_paths)
        total_size_gb = total_size_bytes / (1024**3)

        duration_str = get_readable_time(float(self.media_info['format']['duration'])) if self.media_info and 'format' in self.media_info and 'duration' in self.media_info['format'] else "Unknown"

        vcodec, resolution, audio_langs_str = "N/A", "N/A", "N/A"
        if self.media_info:
            video_streams = [s for s in self.media_info.get('streams', []) if s.get('codec_type') == 'video']
            if video_streams:
                vstream = video_streams[0]
                vcodec = vstream.get('codec_name', 'N/A')
                resolution = f"{vstream.get('width', 'N/A')}x{vstream.get('height', 'N/A')}"

            audio_streams = [s for s in self.media_info.get('streams', []) if s.get('codec_type') == 'audio']
            if audio_streams:
                 audio_langs = {s.get('tags', {}).get('language', 'und') for s in audio_streams}
                 audio_langs_str = ', '.join(lang.upper() for lang in audio_langs) if audio_langs else "N/A"

        part_links = []
        for i, msg in enumerate(sent_messages):
            size_gb = os.path.getsize(file_paths[i]) / (1024**3)
            file_name = ospath.basename(file_paths[i])
            part_links.append(f"**{i+1}.** [{file_name}]({msg.link}) ({size_gb:.2f} GB)")

        summary = (
            f"✅ **Upload Complete: {base_name}**\n\n"
            f"📁 **Uploaded Parts ({len(sent_messages)}):**\n"
            + "\n".join(part_links) + "\n\n"
            f"📊 **Video Info:** `{vcodec}`, `{resolution}`, `{audio_langs_str}`\n"
            f"⏱️ **Duration:** `{duration_str}`\n"
            f"📦 **Total Size:** `{total_size_gb:.2f} GB`\n\n"
            f"🔊 **Audio Tracks Kept:** `{', '.join(self.kept_audio_langs) or 'None'}`\n"
            f"🚫 **Removed Audio:** `{', '.join(self.removed_audio_langs) or 'None'}`\n"
            f"🚫 **Removed Subtitles:** `{', '.join(self.removed_subtitle_langs) or 'None'}`\n\n"
            f"⚡️ @{Config.BOT_USERNAME}"
        )

        try:
            await self.message.reply_text(summary, disable_web_page_preview=True, quote=True)
        except Exception as e:
            LOGGER.error(f"Failed to send master summary: {e}")

    # Other methods like on_upload_complete, on_download_error etc. remain the same.
    # ... (rest of the class)
