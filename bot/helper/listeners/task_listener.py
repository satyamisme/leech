from aiofiles.os import path as aiopath, listdir, remove
from asyncio import sleep, gather
from os import path as ospath, walk
from html import escape
import re

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
from ..ext_utils.bot_utils import sync_to_async, cmd_exec
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
from ..video_utils.executor import VidEcxecutor
from ..telegram_helper.message_utils import (
    send_message,
    delete_status,
    update_status_message,
    send_status_message,
    edit_message,
    delete_message,
)
from time import time
from datetime import datetime
from ..ext_utils.bot_utils import SetInterval
from ..ext_utils.status_utils import get_progress_bar_string

def natural_sort_key(text):
    return [int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', text)]

class TaskListener(TaskConfig):
    def __init__(self):
        super().__init__()
        self.gid = ""
        self.auto_merge = False
        self.auto_split = False
        self.auto_process = False
        self.streams_kept = None
        self.streams_removed = None
        self.media_info = None
        self.status_message = None
        self.start_time = time()
        self.last_progress_text = None
        self.uploaded_files = {}
        self.file_metadata = {}
        self.total_parts = 1
        self.current_part = 1
        self.size = 0
        self.original_name = ""

    async def on_task_created(self):
        self.gid = str(self.mid)
        self.status_message = await send_message(self.message, "🎬 Analyzing Streams... ⏳")

    async def clean(self):
        try:
            if st := intervals.get("status"):
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
                and self.mid in self.same_dir.get(self.folder_name, {}).get("tasks", [])
            ):
                self.same_dir[self.folder_name]["tasks"].remove(self.mid)
                self.same_dir[self.folder_name]["total"] -= 1

    async def on_download_start(self):
        if self.is_super_chat and Config.INCOMPLETE_TASK_NOTIFIER and Config.DATABASE_URL:
            await database.add_incomplete_task(self.message.chat.id, self.message.link, self.tag)

    async def _process_and_upload(self, file_path):
        self.name = ospath.basename(file_path)
        self.original_name = self.name
        self.streams_kept = None
        self.streams_removed = None
        self.media_info = None

        if self.status_message:
            await edit_message(self.status_message, f"🎬 **Processing:** `{self.name}` 🔄")

        self.vidMode = ("rmstream", self.name, {})
        executor = VidEcxecutor(self, file_path, self.gid)
        up_path = await executor.execute()

        if self.is_cancelled or not up_path:
            LOGGER.error(f"Skipping {self.name} due to processing failure.")
            await self._upload_file(file_path)
            return
        self.size = await get_path_size(up_path)

        self.file_metadata[self.name] = {
            'original': self.original_name,
            'streams_kept': self.streams_kept,
            'streams_removed': self.streams_removed,
            'media_info': self.media_info,
            'size': self.size
        }

        split_files = await self.proceed_split(up_path, self.gid)
        if self.is_cancelled:
            return

        upload_dir = ospath.dirname(split_files[0])
        tg = TelegramUploader(self, upload_dir)
        async with task_dict_lock:
            task_dict[self.mid] = TelegramStatus(self, tg, self.gid, "up")

        async for sent_message in tg.upload():
            if self.is_cancelled: break
            if sent_message:
                fname = ospath.basename(sent_message.document.file_name if sent_message.document else sent_message.video.file_name)
                self.uploaded_files[fname] = sent_message
                await self._send_leech_completion_message(sent_message)

    async def _upload_file(self, file_path):
        if self.is_cancelled: return
        if self.status_message: await edit_message(self.status_message, f"📤 **Uploading:** `{ospath.basename(file_path)}`")

        tg = TelegramUploader(self, file_path)
        async with task_dict_lock:
            task_dict[self.mid] = TelegramStatus(self, tg, self.gid, "up")

        async for sent_message in tg.upload():
            if self.is_cancelled: break
            if sent_message:
                await self._send_leech_completion_message(sent_message)

    async def on_download_complete(self):
        await sleep(2)
        if self.is_cancelled: return

        async with task_dict_lock:
            if self.mid not in task_dict: return
            download = task_dict[self.mid]
            self.name = download.name()
            self.gid = download.gid()

        LOGGER.info(f"Download completed: {self.name}")
        dl_path = f"{DOWNLOAD_DIR}{self.mid}/{self.name}"
        up_path = dl_path

        if self.extract:
            up_path = await self.proceed_extract(up_path, self.gid)
            if self.is_cancelled: return

        if await aiopath.isdir(up_path):
            video_files = []
            for root, _, files in await sync_to_async(walk, up_path):
                for file in files:
                    file_path = ospath.join(root, file)
                    if await is_video(file_path):
                        video_files.append(file_path)

            if not video_files:
                await self.on_upload_error("No video files found!")
                return

            video_files.sort(key=natural_sort_key)

            if self.auto_process:
                for file_path in video_files:
                    if self.is_cancelled: break
                    await self._process_and_upload(file_path)
            else:
                await self._upload_file(up_path) # Upload the whole directory
        else:
            if self.auto_process:
                await self._process_and_upload(up_path)
            else:
                await self._upload_file(up_path)

        if self.is_cancelled: return
        if self.status_message: await delete_message(self.status_message)
        await clean_download(f"{DOWNLOAD_DIR}{self.mid}")

    async def _send_leech_completion_message(self, sent_message):
        if not sent_message: return

        name = ospath.basename(sent_message.document.file_name if sent_message.document else sent_message.video.file_name)
        size = sent_message.document.file_size if sent_message.document else sent_message.video.file_size

        base_name = re.sub(r" - Part \d+", "", name)
        base_name = f"{ospath.splitext(base_name)[0]}.mkv"

        meta = self.file_metadata.get(base_name) or self.file_metadata.get(name)
        if meta:
            orig_name = meta.get('original', self.original_name)
            streams_kept = meta.get('streams_kept', [])
            streams_removed = meta.get('streams_removed', [])
            media_info = meta.get('media_info')
            total_size = meta.get('size', self.size)
        else:
            orig_name = self.original_name
            streams_kept = self.streams_kept
            streams_removed = self.streams_removed
            media_info = self.media_info
            total_size = self.size

        total_parts = self.total_parts
        current_part = self.current_part

        msg = f"🎬 <code>{name}</code>"
        msg += f"\n📁 Part {current_part} of {total_parts} | 📂 Total: {get_readable_file_size(total_size)}"

        if media_info:
             msg += f" | ⏱️ {get_readable_time(float(media_info['format']['duration']))}"

        msg += f"\n📡 Source: {self.tag}"
        msg += f"\n\n📽️ <code>{orig_name}</code>"
        msg += f"\n📏 {get_readable_file_size(size)} | 📅 {datetime.fromtimestamp(time()).strftime('%d %b %Y')}"

        if streams_kept:
            msg += "\n\n**Streams Kept:**"
            video_streams_kept = [s for s in streams_kept if s['codec_type'] == 'video' and s.get('disposition', {}).get('attached_pic') == 0]
            audio_streams_kept = [s for s in streams_kept if s['codec_type'] == 'audio']
            if video_streams_kept:
                msg += f"\n🎥 {self._format_stream_info(video_streams_kept[0], 'video')}"
            for stream in audio_streams_kept:
                msg += f"\n🔊 {self._format_stream_info(stream, 'audio')}"

        if streams_removed:
            msg += "\n\n**Streams Removed:**"
            audio_removed = [s for s in streams_removed if s['codec_type'] == 'audio']
            subs_removed = [s for s in streams_removed if s['codec_type'] == 'subtitle']
            for stream in audio_removed:
                msg += f"\n🚫 {self._format_stream_info(stream, 'audio')}"
            for stream in subs_removed:
                msg += f"\n🚫 {self._format_stream_info(stream, 'subtitle')}"

        buttons = ButtonMaker()
        if sent_message.link:
            buttons.url_button("Download Link", sent_message.link)
        reply_markup = buttons.build_menu(2) if buttons._button else None
        await send_message(self.message, msg, reply_markup)

    # ... (rest of the methods are the same)
