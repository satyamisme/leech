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
    is_archive,
)
from ..ext_utils.links_utils import is_gdrive_id
from ..ext_utils.status_utils import get_readable_file_size
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
)


from time import time
from datetime import datetime
from ..telegram_helper.message_utils import send_message, edit_message, delete_message, get_readable_message
from ..ext_utils.bot_utils import SetInterval
from ..ext_utils.status_utils import get_progress_bar_string, get_readable_time

class TaskListener(TaskConfig):
    def __init__(self):
        super().__init__()
        self.streams_kept = None
        self.streams_removed = None
        self.media_info = None
        self.status_message = None
        self.start_time = time()
        self.last_progress_text = None

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

    async def on_download_complete(self):
        if self.is_cancelled:
            return

        gid = getattr(self, "gid", self.mid)

        dl_path = f"{self.dir}/{self.name}"
        up_path = dl_path

        # Step 1: Extract if it's a ZIP/RAR
        if self.extract and is_archive(up_path):
            LOGGER.info(f"Extracting archive: {up_path}")
            up_path = await self.proceed_extract(up_path, gid)
            if not up_path or self.is_cancelled:
                return
            # After extract, up_path is now a directory with extracted files

        # Step 2: If it's a directory (from torrent or extraction), find video files
        if await aiopath.isdir(up_path):
            LOGGER.info(f"Processing directory: {up_path}")
            video_files = []
            async for root, _, files in aiopath.walk(up_path):
                for f in files:
                    fp = ospath.join(root, f)
                    if await is_video(fp):
                        size = await aiopath.getsize(fp)
                        video_files.append((fp, size))
            if not video_files:
                await self.on_upload_error("No video files found in the extracted folder.")
                return

            # Sort by size, largest first
            video_files.sort(key=lambda x: x[1], reverse=True)

            # Process each video file
            for video_file, _ in video_files:
                self.name = ospath.basename(video_file)
                await self._process_single_video(video_file)

        else:
            # Single file
            await self._process_single_video(up_path)

        # Final cleanup
        await clean_download(self.dir)
        if self.up_dir:
            await clean_download(self.up_dir)

    async def _process_single_video(self, up_path):
        if self.is_leech and not self.compress:
            result = await process_video(up_path, self)
            if not result or self.is_cancelled:
                return
            processed_path, media_info = result
            upload_path = processed_path
            self.media_info = media_info
            self.streams_kept = media_info.get("streams_kept", [])
            self.streams_removed = media_info.get("streams_removed", [])
        else:
            upload_path = up_path
            gid = getattr(self, "gid", self.mid)
            if is_gdrive_id(self.up_dest):
                gdrive = GoogleDriveUpload(self)
                async with task_dict_lock:
                    task_dict[self.mid] = GoogleDriveStatus(self, gdrive, gid, "up")
                await send_status_message(self.message)
                await sync_to_async(gdrive.upload, upload_path)
            else:
                rclone = RcloneTransferHelper(self)
                async with task_dict_lock:
                    task_dict[self.mid] = RcloneStatus(self, rclone, gid, "up")
                await send_status_message(self.message)
                await rclone.upload(upload_path)
            return

        tg_uploader = TelegramUploader(self, upload_path)
        async for sent_message in tg_uploader.upload():
            if self.is_cancelled:
                return
            if sent_message:
                await self._send_leech_completion_message(sent_message)

    async def _update_ffmpeg_progress(self):
        if self.status_message is None:
            return
        async with task_dict_lock:
            if self.mid in task_dict:
                task = task_dict[self.mid]
                progress = task.progress()
                text = f"🎬 **Processing Video:** `{self.name}` 🔄\n{get_progress_bar_string(progress)} {progress}"
                if self.last_progress_text != text:
                    self.last_progress_text = text
                    await edit_message(self.status_message, text)

    def _format_stream_info(self, stream, stream_type):
        details = []
        if stream_type == 'video':
            details.append(f"<code>{stream.get('codec_name', 'N/A')}")
            if 'profile' in stream:
                details.append(f"{stream['profile']}")
            details.append(f"{stream.get('height')}p")
            if 'r_frame_rate' in stream:
                fps = stream['r_frame_rate'].split('/')[0]
                details.append(f"{fps}fps</code>")
            return ', '.join(details)

        index = stream.get('index', 'N/A')
        lang = stream.get('tags', {}).get('language', 'N/A').upper()
        codec = stream.get('codec_name', 'N/A')

        if stream_type == 'audio':
            layout = stream.get('channel_layout', 'N/A')

            bitrate_str = stream.get('bit_rate')
            if not bitrate_str:
                bitrate_str = stream.get('tags', {}).get('BPS')
            if not bitrate_str:
                bitrate_str = stream.get('tags', {}).get('bitrate')

            if bitrate_str and bitrate_str.isdigit():
                bitrate = f"{int(bitrate_str) // 1000}kbps"
            else:
                bitrate = 'N/A'

            return f"<code>{index}. {codec} {lang}, {layout}, {bitrate}</code>"

        if stream_type == 'subtitle':
            default = "Default" if stream.get('disposition', {}).get('default') else ""
            return f"<code>{index}. {codec} {lang}, {default}</code>"

    async def _send_leech_completion_message(self, sent_message):
        # This new method only builds and sends the message for a single file.
        name = ospath.basename(sent_message.document.file_name if sent_message.document else sent_message.video.file_name)
        size = sent_message.document.file_size if sent_message.document else sent_message.video.file_size

        total_parts = self.total_parts
        current_part = self.current_part

        msg = f"🎬 <code>{self.name}</code>"
        msg += f"\n📁 Part {current_part} of {total_parts} | 📂 Total: {get_readable_file_size(self.size)} | ⏱️ {get_readable_time(float(self.media_info['format']['duration']))}"

        if self.media_info:
            # Video info
            video_stream = next((stream for stream in self.streams_kept if stream['codec_type'] == 'video'), None)
            if video_stream:
                msg += f"\n📊 {video_stream.get('height')}p • {video_stream.get('codec_name')} • "

            # Audio info
            audio_stream = next((stream for stream in self.streams_kept if stream['codec_type'] == 'audio'), None)
            if audio_stream:
                msg += f"{len(self.streams_kept)}A • {audio_stream.get('tags', {}).get('language', 'N/A').upper()} • Split"

            # Source
            msg += f"\n📡 Source: {self.tag}"

            # Original Filename
            msg += f"\n\n📽️ <code>{self.original_name}</code>"
            msg += f"\n📏 {get_readable_file_size(size)} | 📅 {datetime.fromtimestamp(time()).strftime('%d %b %Y')}"

            # Streams Kept
            msg += "\n\n**Streams Kept:**"
            video_streams_kept = [s for s in self.streams_kept if s['codec_type'] == 'video' and s.get('disposition', {}).get('attached_pic') == 0]
            audio_streams_kept = [s for s in self.streams_kept if s['codec_type'] == 'audio']

            if video_streams_kept:
                vid_info = self._format_stream_info(video_streams_kept[0], 'video')
                msg += f"\n🎥 {vid_info}"
            for stream in audio_streams_kept:
                msg += f"\n🔊 {self._format_stream_info(stream, 'audio')}"

            # Streams Removed
            if self.streams_removed:
                msg += "\n\n**Streams Removed:**"
                audio_removed = [s for s in self.streams_removed if s['codec_type'] == 'audio']
                subs_removed = [s for s in self.streams_removed if s['codec_type'] == 'subtitle']
                for stream in audio_removed:
                    msg += f"\n🚫 {self._format_stream_info(stream, 'audio')}"
                for stream in subs_removed:
                    msg += f"\n🚫 {self._format_stream_info(stream, 'subtitle')}"

            # Navigation
            if current_part > 1:
                prev_part_name = name.replace(f".part{current_part:02d}", f".part{current_part-1:02d}")
                msg += f"\n⬅️ Prev Part: <code>{prev_part_name}</code>"
            if current_part < total_parts:
                next_part_name = name.replace(f".part{current_part:02d}", f".part{current_part+1:02d}")
                msg += f"\n➡️ Next Part: <code>{next_part_name}</code>"

            # Final Summary
            msg += f"\n\n✅ Upload Complete (Part {current_part}/{total_parts})"
            if current_part == total_parts:
                msg += "\n✨ All parts uploaded successfully!"
                msg += f"\n🔗 Files are now available in your chat."
                msg += f"\n⚡️ {self.tag}"

        else:
            # Fallback for non-media files
            msg = f"🎉 <b>Task Completed by {self.tag}</b>"
            msg += f"\n\n<b>Name:</b> <code>{name}</code>"
            msg += f"\n<b>Size:</b> {get_readable_file_size(size)}"
            msg += f"\n\n<b>cc:</b> {self.tag}"

        buttons = ButtonMaker()
        if sent_message.link:
            buttons.url_button("Download Link", sent_message.link)
        reply_markup = buttons.build_menu(2) if buttons._button else None
        try:
            await send_message(sent_message, msg, reply_markup)
        except RPCError as e:
            LOGGER.error(f"Failed to send completion message: {e}")
            if "BUTTON_URL_INVALID" in str(e) and sent_message.link:
                LOGGER.warning("Retrying without the button...")
                await send_message(sent_message, msg)

    async def on_upload_complete(
        self, link, files, folders, mime_type, rclone_path="", dir_id="", tg_sent_messages=None
    ):
        # This method is now primarily for GDrive/Rclone uploads.
        if self.is_super_chat and Config.INCOMPLETE_TASK_NOTIFIER and Config.DATABASE_URL:
            await database.rm_complete_task(self.message.link)

        msg = f"🎉 <b>Task Completed by {self.tag}</b>"
        msg += f"\n\n<b>Name:</b> <code>{self.name}</code>"
        msg += f"\n<b>Size:</b> {get_readable_file_size(self.size)}"
        msg += f"\n\n<b>cc:</b> {self.tag}"

        if self.status_message:
            await delete_message(self.status_message)

        buttons = ButtonMaker()
        if link:
            buttons.url_button("Cloud Link", link)
        reply_markup = buttons.build_menu(2) if buttons._button else None
        await send_message(self.message, msg, reply_markup)

        if self.seed:
            await clean_target(self.up_dir)
            async with queue_dict_lock:
                if self.mid in non_queued_up:
                    non_queued_up.remove(self.mid)
            await start_from_queued()
            return
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

    async def on_download_error(self, error, button=None):
        async with task_dict_lock:
            if self.mid in task_dict:
                del task_dict[self.mid]
            count = len(task_dict)
        await self.remove_from_same_dir()
        msg = f"{self.tag} Download: {escape(str(error))}"
        await send_message(self.message, msg, button)
        if count == 0:
            await self.clean()
        else:
            await update_status_message(self.message.chat.id)

        if (
            self.is_super_chat
            and Config.INCOMPLETE_TASK_NOTIFIER
            and Config.DATABASE_URL
        ):
            await database.rm_complete_task(self.message.link)

        async with queue_dict_lock:
            if self.mid in queued_dl:
                queued_dl[self.mid].set()
                del queued_dl[self.mid]
            if self.mid in queued_up:
                queued_up[self.mid].set()
                del queued_up[self.mid]
            if self.mid in non_queued_dl:
                non_queued_dl.remove(self.mid)
            if self.mid in non_queued_up:
                non_queued_up.remove(self.mid)

        await start_from_queued()
        await sleep(3)
        await clean_download(self.dir)
        if self.up_dir:
            await clean_download(self.up_dir)
        if self.thumb and await aiopath.exists(self.thumb):
            await remove(self.thumb)

    async def on_upload_error(self, error):
        async with task_dict_lock:
            if self.mid in task_dict:
                del task_dict[self.mid]
            count = len(task_dict)
        await send_message(self.message, f"{self.tag} {escape(str(error))}")
        if count == 0:
            await self.clean()
        else:
            await update_status_message(self.message.chat.id)

        if (
            self.is_super_chat
            and Config.INCOMPLETE_TASK_NOTIFIER
            and Config.DATABASE_URL
        ):
            await database.rm_complete_task(self.message.link)

        async with queue_dict_lock:
            if self.mid in queued_dl:
                queued_dl[self.mid].set()
                del queued_dl[self.mid]
            if self.mid in queued_up:
                queued_up[self.mid].set()
                del queued_up[self.mid]
            if self.mid in non_queued_dl:
                non_queued_dl.remove(self.mid)
            if self.mid in non_queued_up:
                non_queued_up.remove(self.mid)

        await start_from_queued()
        await sleep(3)
        await clean_download(self.dir)
        if self.up_dir:
            await clean_download(self.up_dir)
        if self.thumb and await aiopath.exists(self.thumb):
            await remove(self.thumb)
