from aiofiles.os import path as aiopath, listdir, remove
from asyncio import sleep, gather
from os import path as ospath
from html import escape
from requests import utils as rutils
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
from natsort import natsorted
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
    async def _split_large_file(self, file_path):
        if not await aiopath.isfile(file_path):
            return file_path
        size = await aiopath.getsize(file_path)
        if size <= 2000000000:  # 2GB
            return file_path
        LOGGER.info(f"Splitting large file: {file_path}")
        dir_path = ospath.dirname(file_path)
        base_name = ospath.basename(file_path)
        cmd = ["split", "-b", "1900M", file_path, f"{base_name}.part"]
        try:
            _, stderr, code = await cmd_exec(cmd)
            if code == 0:
                if await aiopath.exists(file_path):
                    await remove(file_path)
                return dir_path
            else:
                LOGGER.error(f"Generic split failed: {stderr.decode().strip()}")
                return file_path
        except Exception as e:
            LOGGER.error(f"Error during split: {e}")
            return file_path

    def __init__(self):
        super().__init__()
        self.gid = ""
        self.streams_kept = None
        self.streams_removed = None
        self.media_info = None
        self.status_message = None
        self.start_time = time()
        self.file_metadata = {}
        self.original_name = ""
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
            self.gid = gid
        LOGGER.info(f"Download completed: {self.name}")

        if multi_links:
            await self.on_upload_error(f"{self.name} Downloaded!\n\nWaiting for other tasks to finish...")
            return

        dl_path = f"{self.dir}/{self.name}"

        from ..ext_utils.files_utils import is_archive
        from ..ext_utils.mkvmerge_utils import split_video_if_needed
        from aiofiles.os import rename as aiorename

        up_path = dl_path
        if self.extract or await sync_to_async(is_archive, up_path):
            up_path = await self.proceed_extract(up_path, gid)
            if self.is_cancelled: return

        self.name = ospath.basename(up_path)
        LOGGER.info(f"Processing: {self.name}")

        if self.is_leech and not self.compress:
            if await aiopath.isdir(up_path):
                for f in natsorted(await listdir(up_path)):
                    file_path = ospath.join(up_path, f)
                    if not await aiopath.isfile(file_path): continue

                    if await is_video(file_path):
                        result = await process_video(file_path, self)
                        if self.is_cancelled: return
                        if isinstance(result, tuple) and result[0]:
                            processed_file, self.media_info = result
                            if processed_file != file_path:
                                await aiorename(processed_file, file_path)
                        await split_video_if_needed(file_path)
                    else:
                        await self._split_large_file(file_path)
                    if self.is_cancelled: return
            elif await aiopath.isfile(up_path):
                if await is_video(up_path):
                    result = await process_video(up_path, self)
                    if self.is_cancelled: return
                    if isinstance(result, tuple) and result[0]:
                        up_path, self.media_info = result
                    elif isinstance(result, str):
                        up_path = result
                    self.name = ospath.basename(up_path)
                    await split_video_if_needed(up_path)
                else:
                    up_path = await self._split_large_file(up_path)

        if self.join:
            await join_files(up_path)
        if self.name_sub:
            up_path = await self.substitute(up_path)
            self.name = ospath.basename(up_path)
        if self.compress:
            up_path = await self.proceed_compress(up_path, gid)
            if self.is_cancelled: return
            self.name = ospath.basename(up_path)

        if self.is_leech and not self.compress:
            self.clear()

        self.size = await get_path_size(up_path)
        if self.size == 0:
            await self.on_upload_error("File size is zero")
            return

        add_to_queue, event = await check_running_tasks(self, "up")
        if add_to_queue:
            LOGGER.info(f"Added to Queue/Upload: {self.name}")
            async with task_dict_lock:
                task_dict[self.mid] = QueueStatus(self, gid, "Up")
            await event.wait()
            if self.is_cancelled: return
            LOGGER.info(f"Start from Queued/Upload: {self.name}")

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

            self.total_parts = tg.total_parts
            async for sent_message in tg.upload():
                if self.is_cancelled:
                    break
                if sent_message:
                    self.current_part = tg.current_part
                    await self._send_leech_completion_message(sent_message)

            if self.is_cancelled:
                return

            # Final cleanup for leech tasks
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

        elif is_gdrive_id(self.up_dest):
            LOGGER.info(f"Gdrive Upload Name: {self.name}")
            drive = GoogleDriveUpload(self, up_path)
            async with task_dict_lock:
                task_dict[self.mid] = GoogleDriveStatus(self, drive, gid, "up")
            await sync_to_async(drive.upload)
        else:
            LOGGER.info(f"Rclone Upload Name: {self.name}")
            RCTransfer = RcloneTransferHelper(self)
            async with task_dict_lock:
                task_dict[self.mid] = RcloneStatus(self, RCTransfer, gid, "up")
            await RCTransfer.upload(up_path)

    async def _update_ffmpeg_progress(self):
        if self.status_message is None:
            return
        async with task_dict_lock:
            if self.mid in task_dict:
                task = task_dict[self.mid]
                progress = task.progress()
                text = f"🎬 **Processing Video:** `{self.name}` 🔄\n{get_progress_bar_string(progress)} {progress}"
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
        name = ospath.basename(sent_message.document.file_name if sent_message.document else sent_message.video.file_name)
        size = sent_message.document.file_size if sent_message.document else sent_message.video.file_size
        total_parts = self.total_parts
        current_part = self.current_part
        msg = f"🎬 <code>{self.name}</code>"
        msg += f"\n📁 Part {current_part} of {total_parts} | 📂 Total: {get_readable_file_size(self.size)} | ⏱️ {get_readable_time(float(self.media_info['format']['duration']))}"
        if self.media_info:
            video_stream = next((stream for stream in self.streams_kept if stream['codec_type'] == 'video'), None)
            if video_stream:
                msg += f"\n📊 {video_stream.get('height')}p • {video_stream.get('codec_name')} • "
            audio_stream = next((stream for stream in self.streams_kept if stream['codec_type'] == 'audio'), None)
            if audio_stream:
                msg += f"{len(self.streams_kept)}A • {audio_stream.get('tags', {}).get('language', 'N/A').upper()} • Split"
            msg += f"\n📡 Source: {self.tag}"
            msg += f"\n\n📽️ <code>{self.original_name}</code>"
            msg += f"\n📏 {get_readable_file_size(size)} | 📅 {datetime.fromtimestamp(time()).strftime('%d %b %Y')}"
            msg += "\n\n**Streams Kept:**"
            video_streams_kept = [s for s in self.streams_kept if s['codec_type'] == 'video' and s.get('disposition', {}).get('attached_pic') == 0]
            audio_streams_kept = [s for s in self.streams_kept if s['codec_type'] == 'audio']
            if video_streams_kept:
                vid_info = self._format_stream_info(video_streams_kept[0], 'video')
                msg += f"\n🎥 {vid_info}"
            for stream in audio_streams_kept:
                msg += f"\n🔊 {self._format_stream_info(stream, 'audio')}"
            if self.streams_removed:
                msg += "\n\n**Streams Removed:**"
                audio_removed = [s for s in self.streams_removed if s['codec_type'] == 'audio']
                subs_removed = [s for s in self.streams_removed if s['codec_type'] == 'subtitle']
                for stream in audio_removed:
                    msg += f"\n🚫 {self._format_stream_info(stream, 'audio')}"
                for stream in subs_removed:
                    msg += f"\n🚫 {self._format_stream_info(stream, 'subtitle')}"
        else:
            msg = f"🎉 <b>Task Completed by {self.tag}</b>\n\n<b>Name:</b> <code>{name}</code>\n<b>Size:</b> {get_readable_file_size(size)}\n\n<b>cc:</b> {self.tag}"
        buttons = ButtonMaker()
        if sent_message.link:
            buttons.url_button("Download Link", sent_message.link)
        reply_markup = buttons.build_menu(2) if buttons._button else None
        await send_message(self.message, msg, reply_markup)

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
