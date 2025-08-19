# task_listener.py

from aiofiles.os import path as aiopath, listdir, remove
from asyncio import sleep, gather
from os import path as ospath, walk
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
from ..video_utils.processor import process_video
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

import re
import os

# Natural sort for correct S01E01, S01E02, ..., S01E10 ordering
def natural_sort_key(text):
    return [int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', text)]

class TaskListener(TaskConfig):
    def __init__(self):
        super().__init__()
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
        self.file_metadata = {}  # filename → {kept, removed, size, duration, original}
        self.total_parts = 1
        self.current_part = 1
        self.size = 0

    async def on_task_created(self):
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

    async def _merge_videos(self, video_files, output_dir):
        if len(video_files) == 1:
            return video_files[0]

        from bot.helper.ext_utils.bot_utils import cmd_exec
        import os

        base_name = os.path.splitext(os.path.basename(video_files[0]))[0]
        # Remove episode info if present
        series_pattern = re.compile(r'(.*?)S?\d{1,2}E?\d{1,3}', re.I)
        match = series_pattern.match(base_name)
        merge_name = match.group(1).strip() if match else base_name
        merged_path = os.path.join(output_dir, f"{merge_name}.merged.mkv")

        cmd = ["mkvmerge", "-o", merged_path] + video_files
        _, stderr, code = await cmd_exec(cmd)

        if code != 0:
            LOGGER.error(f"Merge failed: {stderr}")
            return None

        # Cleanup originals
        for f in video_files:
            if f != merged_path:
                try:
                    os.remove(f)
                except:
                    pass

        return merged_path

    async def _process_and_upload(self, file_path):
        if file_path.lower().endswith(('.zip', '.rar', '.7z', '.tar', '.gz')):
            LOGGER.info(f"Skipping archive file: {file_path}")
            return

        self.name = ospath.basename(file_path)
        self.original_name = self.name
        self.streams_kept = None
        self.streams_removed = None
        self.media_info = None

        if self.status_message:
            await edit_message(self.status_message, f"🎬 **Processing:** `{self.name}` 🔄")

        interval = SetInterval(3, self._update_ffmpeg_progress)
        result = await process_video(file_path, self)
        interval.cancel()

        if self.is_cancelled or result is None or (isinstance(result, tuple) and result[0] is None):
            LOGGER.error(f"Skipping {self.name}")
            return

        if isinstance(result, tuple) and result[0] is not None:
            up_path = result[0]
            self.media_info = result[1]
        else:
            up_path = file_path

        self.size = await get_path_size(up_path)
        self.file_metadata[self.name] = {
            'name': self.name,
            'original': self.original_name,
            'media_info': self.media_info,
            'streams_kept': self.streams_kept,
            'streams_removed': self.streams_removed,
            'size': self.size
        }

        if self.is_leech and not self.compress:
            await self.proceed_split(up_path, self.gid)
            if self.is_cancelled: return
            upload_dir = ospath.dirname(up_path)
            tg = TelegramUploader(self, upload_dir)
            async with task_dict_lock:
                task_dict[self.mid] = TelegramStatus(self, tg, self.gid, "up")
            async for sent_message in tg.upload():
                if self.is_cancelled: break
                if sent_message:
                    fname = ospath.basename(sent_message.document.file_name)
                    self.uploaded_files[fname] = sent_message
                    await self._send_leech_completion_message(sent_message)

    async def on_download_complete(self):
        await sleep(2)
        if self.is_cancelled:
            return

        multi_links = False
        if self.folder_name and self.same_dir and self.mid in self.same_dir.get(self.folder_name, {}).get("tasks", []):
            async with same_directory_lock:
                while True:
                    async with task_dict_lock:
                        if self.mid not in self.same_dir[self.folder_name]["tasks"]:
                            return
                        if self.same_dir[self.folder_name]["total"] <= 1 or len(self.same_dir[self.folder_name]["tasks"]) > 1:
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
            if self.is_cancelled or self.mid not in task_dict:
                return
            download = task_dict[self.mid]
            self.name = download.name()
            gid = download.gid()

        LOGGER.info(f"Download completed: {self.name}")
        if multi_links:
            await self.on_upload_error(f"{self.name} Downloaded!\nWaiting for other tasks to finish...")
            return

        dl_path = f"{self.dir}/{self.name}"
        up_path = dl_path

        if self.extract:
            up_path = await self.proceed_extract(up_path, gid)
            if self.is_cancelled:
                return
            self.name = up_path.replace(f"{self.dir}/", "").split("/", 1)[0]

        if await aiopath.isdir(up_path):
            video_files = []
            async for root, _, files in async_walk(up_path):
                for file in files:
                    file_path = ospath.join(root, file)
                    if await is_video(file_path):
                        video_files.append(file_path)
            video_files.sort(key=natural_sort_key)

            if not video_files:
                await self.on_upload_error("No video files found!")
                return

            if self.auto_merge:
                merged_path = await self._merge_videos(video_files, up_path)
                if not merged_path or self.is_cancelled:
                    return
                up_path = merged_path
                self.name = os.path.basename(merged_path)
                self.original_name = " + ".join([os.path.basename(f) for f in video_files[:3]])
                if len(video_files) > 3:
                    self.original_name += f" + {len(video_files)-3} more"
                await self._process_and_upload(up_path)
            elif self.auto_split:
                for file_path in video_files:
                    await self._process_and_upload(file_path)
            else:
                # Default: process first video
                await self._process_and_upload(video_files[0])
        else:
            if self.auto_process:
                await self._process_and_upload(up_path)
            else:
                if self.is_leech:
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
                            fname = ospath.basename(sent_message.document.file_name)
                            self.uploaded_files[fname] = sent_message
                            await self._send_leech_completion_message(sent_message)
                else:
                    # Handle GDrive/Rclone
                    self.size = await get_path_size(up_path)
                    if is_gdrive_id(self.up_dest):
                        drive = GoogleDriveUpload(self, up_path)
                        async with task_dict_lock:
                            task_dict[self.mid] = GoogleDriveStatus(self, drive, gid, "up")
                        await sync_to_async(drive.upload)
                    else:
                        RCTransfer = RcloneTransferHelper(self)
                        async with task_dict_lock:
                            task_dict[self.mid] = RcloneStatus(self, RCTransfer, gid, "up")
                        await RCTransfer.upload(up_path)

        if self.is_cancelled:
            return

        if self.status_message:
            await delete_message(self.status_message)

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

        if not self.is_cancelled and self.message:
            try:
                await delete_message(self.message)
            except Exception as e:
                LOGGER.warning(f"Failed to delete command message: {e}")

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
        if stream_type == 'video':
            details = [f"<code>{stream.get('codec_name', 'N/A')}"]
            if 'profile' in stream:
                details.append(f"{stream['profile']}")
            details.append(f"{stream.get('height')}p")
            if 'r_frame_rate' in stream:
                fps = stream['r_frame_rate'].split('/')[0]
                details.append(f"{fps}fps</code>")
            return ', '.join(details)
        if stream_type == 'audio':
            index = stream.get('index', 'N/A')
            lang = stream.get('tags', {}).get('language', 'N/A').upper()
            codec = stream.get('codec_name', 'N/A')
            layout = stream.get('channel_layout', 'N/A')
            bitrate_str = stream.get('bit_rate') or stream.get('tags', {}).get('BPS') or stream.get('tags', {}).get('bitrate')
            bitrate = f"{int(bitrate_str) // 1000}kbps" if bitrate_str and bitrate_str.isdigit() else 'N/A'
            return f"<code>{index}. {codec} {lang}, {layout}, {bitrate}</code>"
        if stream_type == 'subtitle':
            index = stream.get('index', 'N/A')
            lang = stream.get('tags', {}).get('language', 'N/A').upper()
            codec = stream.get('codec_name', 'N/A')
            default = "Default" if stream.get('disposition', {}).get('default') else ""
            return f"<code>{index}. {codec} {lang}, {default}</code>"

    async def _send_leech_completion_message(self, sent_message):
        name = ospath.basename(sent_message.document.file_name if sent_message.document else sent_message.video.file_name)
        size = sent_message.document.file_size if sent_message.document else sent_message.video.file_size

        # Extract base name without part suffix
        base_name = re.sub(r" - Part \d+", "", name)
        base_name = re.sub(r"\.part\d+", "", base_name)
        base_name = f"{os.path.splitext(base_name)[0]}.mkv"

        meta = self.file_metadata.get(base_name) or self.file_metadata.get(name)

        if meta:
            streams_kept = meta['streams_kept']
            streams_removed = meta['streams_removed']
            media_info = meta['media_info']
            orig_name = meta['original']
            total_size = meta['size']
        else:
            # Fallback to self (for merged files)
            streams_kept = self.streams_kept
            streams_removed = self.streams_removed
            media_info = self.media_info
            orig_name = self.original_name
            total_size = self.size

        # Detect part number
        match = re.search(r"Part (\d+) of (\d+)", name)
        current_part = int(match.group(1)) if match else 1
        total_parts = int(match.group(2)) if match else 1

        # Build message
        msg = f"🎬 <code>{name}</code>"
        if media_info:
            duration = media_info['format']['duration']
            msg += f"\n📁 Part {current_part} of {total_parts} | 📂 Total: {get_readable_file_size(total_size)} | ⏱️ {get_readable_time(float(duration))}"

            video_stream = next((s for s in streams_kept if s['codec_type'] == 'video' and s.get('disposition', {}).get('attached_pic') == 0), None)
            audio_stream = next((s for s in streams_kept if s['codec_type'] == 'audio'), None)

            if video_stream:
                msg += f"\n📊 {video_stream.get('height')}p • {video_stream.get('codec_name')} • "
            if audio_stream:
                msg += f"{len([s for s in streams_kept if s['codec_type'] == 'audio'])}A • {audio_stream.get('tags', {}).get('language', 'N/A').upper()} • Split"
            msg += f"\n📡 Source: {self.tag}"
            msg += f"\n📽️ <code>{orig_name}</code>"
            msg += f"\n📏 {get_readable_file_size(size)} | 📅 {datetime.fromtimestamp(time()).strftime('%d %b %Y')}"

            msg += "\n**Streams Kept:**"
            if video_stream:
                msg += f"\n🎥 {self._format_stream_info(video_stream, 'video')}"
            for s in [s for s in streams_kept if s['codec_type'] == 'audio']:
                msg += f"\n🔊 {self._format_stream_info(s, 'audio')}"

            if streams_removed:
                msg += "\n**Streams Removed:**"
                for s in [s for s in streams_removed if s['codec_type'] == 'audio']:
                    msg += f"\n🚫 {self._format_stream_info(s, 'audio')}"
                for s in [s for s in streams_removed if s['codec_type'] == 'subtitle']:
                    msg += f"\n🚫 {self._format_stream_info(s, 'subtitle')}"

            if current_part > 1:
                prev_name = name.replace(f"Part {current_part}", f"Part {current_part-1}")
                if prev_name in self.uploaded_files:
                    msg += f"\n⬅️ <a href='{self.uploaded_files[prev_name].link}'>Prev Part</a>"
                else:
                    msg += f"\n⬅️ Prev Part: <code>{prev_name}</code>"
            if current_part < total_parts:
                next_name = name.replace(f"Part {current_part}", f"Part {current_part+1}")
                msg += f"\n➡️ Next Part: <code>{next_name}</code>"

            msg += f"\n✅ Upload Complete (Part {current_part}/{total_parts})"
            if current_part == total_parts:
                msg += "\n✨ All parts uploaded successfully!"
                msg += f"\n🔗 Files are now available in your chat."
                msg += f"\n⚡️ {self.tag}"
        else:
            msg = f"🎉 <b>Task Completed by {self.tag}</b>\n<b>Name:</b> <code>{name}</code>\n<b>Size:</b> {get_readable_file_size(size)}\n<b>cc:</b> {self.tag}"

        buttons = ButtonMaker()
        if sent_message.link:
            buttons.url_button("Download Link", sent_message.link)
        reply_markup = buttons.build_menu(2) if buttons._button else None

        try:
            await send_message(sent_message, msg, reply_markup)
        except Exception as e:
            LOGGER.error(f"Failed to send completion message: {e}")
            await send_message(sent_message, msg)

    async def on_upload_complete(self, link, files, folders, mime_type, rclone_path="", dir_id="", tg_sent_messages=None):
        if self.is_super_chat and Config.INCOMPLETE_TASK_NOTIFIER and Config.DATABASE_URL:
            await database.rm_complete_task(self.message.link)
        msg = f"🎉 <b>Task Completed by {self.tag}</b>\n<b>Name:</b> <code>{self.name}</code>\n<b>Size:</b> {get_readable_file_size(self.size)}\n<b>cc:</b> {self.tag}"
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
        if not self.is_cancelled and self.message:
            try:
                await delete_message(self.message)
            except Exception as e:
                LOGGER.warning(f"Failed to delete command message: {e}")

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
        if self.is_super_chat and Config.INCOMPLETE_TASK_NOTIFIER and Config.DATABASE_URL:
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
        if self.is_super_chat and Config.INCOMPLETE_TASK_NOTIFIER and Config.DATABASE_URL:
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
