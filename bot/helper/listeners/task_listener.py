from aiofiles.os import path as aiopath, listdir, remove
from asyncio import sleep, gather
from base64 import b64encode
from os import path as ospath
from html import escape
from re import match as re_match
from requests import utils as rutils
from pyrogram.errors import RPCError
from time import time
from datetime import datetime

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
from ..ext_utils.bot_utils import sync_to_async, get_content_type, SetInterval
from ..ext_utils.db_handler import database
from ..ext_utils.exceptions import DirectDownloadLinkException
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
    get_base_name,
    extract_archive,
)
from ..ext_utils.links_utils import is_gdrive_id, is_magnet, is_rclone_path, is_gdrive_link
from ..ext_utils.status_utils import get_readable_file_size, get_progress_bar_string, get_readable_time
from ..ext_utils.task_manager import start_from_queued, check_running_tasks
from ..mirror_leech_utils.download_utils.aria2_download import add_aria2_download
from ..mirror_leech_utils.download_utils.direct_downloader import add_direct_download
from ..mirror_leech_utils.download_utils.direct_link_generator import (
    direct_link_generator,
)
from ..mirror_leech_utils.download_utils.gd_download import add_gd_download
from ..mirror_leech_utils.download_utils.jd_download import add_jd_download
from ..mirror_leech_utils.download_utils.nzb_downloader import add_nzb
from ..mirror_leech_utils.download_utils.qbit_download import add_qb_torrent
from ..mirror_leech_utils.download_utils.rclone_download import add_rclone_download
from ..mirror_leech_utils.gdrive_utils.upload import GoogleDriveUpload
from ..mirror_leech_utils.rclone_utils.transfer import RcloneTransferHelper
from ..mirror_leech_utils.status_utils.gdrive_status import GoogleDriveStatus
from ..mirror_leech_utils.status_utils.queue_status import QueueStatus
from ..mirror_leech_utils.status_utils.rclone_status import RcloneStatus
from ..mirror_leech_utils.status_utils.telegram_status import TelegramStatus
from ..telegram_helper.button_build import ButtonMaker
from ..video_utils.processor import process_video
from ..telegram_helper.message_utils import (
    send_message,
    delete_status,
    update_status_message,
    send_status_message,
    edit_message,
    delete_message,
    get_readable_message,
)

class TaskListener(TaskConfig):
    def __init__(self):
        super().__init__()
        self.streams_kept = None
        self.streams_removed = None
        self.media_info = None
        self.status_message = None
        self.start_time = time()
        self.last_progress_text = None
        self.args = None
        self.headers = []
        self.ratio = None
        self.seed_time = None
        self.reply_to = None
        self.file_ = None
        self.session = ""
        self.path = ""
        self.total_parts = 1
        self.current_part = 1
        self.original_name = ""

    async def on_task_created(self):
        self.status_message = await send_message(self.message, "🎬 Analyzing Streams... ⏳")
        if self.status_message and hasattr(self.status_message, "id"):
            LOGGER.info(f"Task {self.mid}: Created status message {self.status_message.id}.")
        await self.start_download()

    async def update_and_log_status(self, new_status):
        if not self.status_message:
            LOGGER.warning(f"Task {self.mid}: No status message to update with '{new_status}'.")
            return
        LOGGER.info(f"Task {self.mid}: Updating status to '{new_status}'.")
        try:
            await edit_message(self.status_message, f"**{new_status}**\n\n<code>{self.name}</code>")
        except Exception as e:
            LOGGER.error(f"Task {self.mid}: Failed to edit status message: {e}", exc_info=True)

    async def start_download(self):
        if (
            not self.is_jd
            and not self.is_nzb
            and not self.is_qbit
            and not is_magnet(self.link)
            and not is_rclone_path(self.link)
            and not is_gdrive_link(self.link)
            and not self.link.endswith(".torrent")
            and self.file_ is None
            and not is_gdrive_id(self.link)
        ):
            content_type = await get_content_type(self.link)
            if content_type is None or re_match(r"text/html|text/plain", content_type):
                try:
                    self.link = await sync_to_async(direct_link_generator, self.link)
                    if isinstance(self.link, tuple):
                        self.link, self.headers = self.link
                    elif isinstance(self.link, str):
                        LOGGER.info(f"Generated link: {self.link}")
                except DirectDownloadLinkException as e:
                    e = str(e)
                    if "This link requires a password!" not in e:
                        LOGGER.info(e)
                    if e.startswith("ERROR:"):
                        await send_message(self.message, e)
                        await self.remove_from_same_dir()
                        return
                except Exception as e:
                    await send_message(self.message, e)
                    await self.remove_from_same_dir()
                    return
        await self.initiate_download()

    async def initiate_download(self):
        if self.file_ is not None:
            from ..mirror_leech_utils.download_utils.telegram_download import (
                TelegramDownloadHelper,
            )
            await TelegramDownloadHelper(self).add_download(
                self.reply_to, f"{self.path}/", self.session
            )
        elif isinstance(self.link, dict):
            await add_direct_download(self, self.path)
        elif self.is_jd:
            await add_jd_download(self, self.path)
        elif self.is_qbit:
            await add_qb_torrent(self, self.path, self.ratio, self.seed_time)
        elif self.is_nzb:
            await add_nzb(self, self.path)
        elif is_rclone_path(self.link):
            await add_rclone_download(self, f"{self.path}/")
        elif is_gdrive_link(self.link) or is_gdrive_id(self.link):
            await add_gd_download(self, self.path)
        else:
            ussr = self.args.get("-au", "") if self.args else ""
            pssw = self.args.get("-ap", "") if self.args else ""
            if ussr or pssw:
                auth = f"{ussr}:{pssw}"
                self.headers.extend(
                    [f"authorization: Basic {b64encode(auth.encode()).decode('ascii')}"]
                )
            await add_aria2_download(self, self.path, self.headers, self.ratio, self.seed_time)

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
                hasattr(self, 'folder_name')
                and self.folder_name
                and hasattr(self, 'same_dir')
                and self.same_dir
                and self.mid in self.same_dir.get(self.folder_name, {}).get("tasks", set())
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

    async def proceed_extract(self, up_path, gid):
        try:
            return await extract_archive(up_path, f"{self.dir}/{gid}")
        except Exception as e:
            await self.on_upload_error(str(e))
            return None

    async def on_download_complete(self):
        try:
            if self.is_cancelled:
                return

            dl_path = f"{self.dir}/{self.name}"
            up_path = dl_path

            if self.extract and is_archive(up_path):
                LOGGER.info(f"Extracting archive: {up_path}")
                up_path = await self.proceed_extract(up_path, self.gid)
                if not up_path or self.is_cancelled:
                    return

            if await aiopath.isdir(up_path):
                LOGGER.info(f"Processing directory: {up_path}")
                video_files = []
                for root, _, files in await sync_to_async(ospath.walk, up_path):
                    for f in files:
                        fp = ospath.join(root, f)
                        if await is_video(fp):
                            size = await aiopath.getsize(fp)
                            video_files.append((fp, size))
                if not video_files:
                    await self.on_upload_error("No video files found in the extracted folder.")
                    return

                video_files.sort(key=lambda x: x[1], reverse=True)
                self.total_parts = len(video_files)
                self.current_part = 1
                for video_file, _ in video_files:
                    self.name = ospath.basename(video_file)
                    self.original_name = self.name
                    await self._process_single_video(video_file)
                    self.current_part += 1
            else:
                self.original_name = self.name
                await self._process_single_video(up_path)

            await clean_download(self.dir)
            if hasattr(self, 'up_dir') and self.up_dir:
                await clean_download(self.up_dir)
        finally:
            if self.is_leech:
                if self.status_message:
                    if hasattr(self.status_message, "id"):
                        LOGGER.info(f"Task {self.mid}: Deleting status message {self.status_message.id} on leech completion.")
                    await delete_message(self.status_message)
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

    async def _process_single_video(self, up_path):
        from ..mirror_leech_utils.telegram_uploader import TelegramUploader
        if self.is_leech and not self.compress:
            result = await process_video(up_path, self)
            if self.is_cancelled:
                return
            if result is None:
                return
            processed_path, media_info = result
            if not processed_path:
                return
            upload_path = processed_path
            self.media_info = media_info
            if media_info:
                self.streams_kept = media_info.get("streams_kept", [])
                self.streams_removed = media_info.get("streams_removed", [])
        else:
            upload_path = up_path

        tg_uploader = TelegramUploader(self, upload_path)
        async for sent_message in tg_uploader.upload():
            if self.is_cancelled:
                return
            if sent_message:
                await self._send_leech_completion_message(sent_message)

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

        msg = f"🎬 <code>{self.name}</code>"
        msg += f"\n📁 Part {self.current_part} of {self.total_parts} | 📂 Total: {get_readable_file_size(self.size)}"

        if self.media_info:
            msg += f" | ⏱️ {get_readable_time(float(self.media_info['format']['duration']))}"
            video_stream = next((s for s in self.streams_kept if s['codec_type'] == 'video'), None)
            if video_stream:
                msg += f"\n📊 {video_stream.get('height')}p • {video_stream.get('codec_name')} • "
            audio_stream = next((s for s in self.streams_kept if s['codec_type'] == 'audio'), None)
            if audio_stream:
                msg += f"{len(self.streams_kept)}A • {audio_stream.get('tags', {}).get('language', 'N/A').upper()} • Split"
            msg += f"\n📡 Source: {self.tag}"
            msg += f"\n\n📽️ <code>{self.original_name}</code>"
            msg += f"\n📏 {get_readable_file_size(size)} | 📅 {datetime.fromtimestamp(time()).strftime('%d %b %Y')}"
            msg += "\n\n**Streams Kept:**"
            video_streams_kept = [s for s in self.streams_kept if s['codec_type'] == 'video' and s.get('disposition', {}).get('attached_pic') == 0]
            audio_streams_kept = [s for s in self.streams_kept if s['codec_type'] == 'audio']
            if video_streams_kept:
                msg += f"\n🎥 {self._format_stream_info(video_streams_kept[0], 'video')}"
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
            if self.current_part > 1:
                prev_part_name = name.replace(f".part{self.current_part:02d}", f".part{self.current_part-1:02d}")
                msg += f"\n⬅️ Prev Part: <code>{prev_part_name}</code>"
            if self.current_part < self.total_parts:
                next_part_name = name.replace(f".part{self.current_part:02d}", f".part{self.current_part+1:02d}")
                msg += f"\n➡️ Next Part: <code>{next_part_name}</code>"
            msg += f"\n\n✅ Upload Complete (Part {self.current_part}/{self.total_parts})"
            if self.current_part == self.total_parts:
                msg += "\n✨ All parts uploaded successfully!"
                msg += f"\n🔗 Files are now available in your chat."
                msg += f"\n⚡️ {self.tag}"
        else:
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
        try:
            if self.is_super_chat and Config.INCOMPLETE_TASK_NOTIFIER and Config.DATABASE_URL:
                await database.rm_complete_task(self.message.link)
            msg = f"🎉 <b>Task Completed by {self.tag}</b>"
            msg += f"\n\n<b>Name:</b> <code>{self.name}</code>"
            msg += f"\n<b>Size:</b> {get_readable_file_size(self.size)}"
            msg += f"\n\n<b>cc:</b> {self.tag}"
            buttons = ButtonMaker()
            if link:
                buttons.url_button("Cloud Link", link)
            reply_markup = buttons.build_menu(2) if buttons._button else None
            await send_message(self.message, msg, reply_markup)
            if self.seed:
                if hasattr(self, 'up_dir'):
                    await clean_target(self.up_dir)
                async with queue_dict_lock:
                    if self.mid in non_queued_up:
                        non_queued_up.remove(self.mid)
                await start_from_queued()
                return
            await clean_download(self.dir)
        finally:
            if self.status_message:
                if hasattr(self.status_message, "id"):
                    LOGGER.info(f"Task {self.mid}: Deleting status message {self.status_message.id} on upload completion.")
                await delete_message(self.status_message)
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
        if self.status_message:
            if hasattr(self.status_message, "id"):
                LOGGER.info(f"Task {self.mid}: Deleting status message {self.status_message.id} on download error.")
            await delete_message(self.status_message)
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
        if hasattr(self, 'up_dir') and self.up_dir:
            await clean_download(self.up_dir)
        if self.thumb and await aiopath.exists(self.thumb):
            await remove(self.thumb)

    async def on_upload_error(self, error):
        async with task_dict_lock:
            if self.mid in task_dict:
                del task_dict[self.mid]
            count = len(task_dict)
        await send_message(self.message, f"{self.tag} {escape(str(error))}")
        if self.status_message:
            if hasattr(self.status_message, "id"):
                LOGGER.info(f"Task {self.mid}: Deleting status message {self.status_message.id} on upload error.")
            await delete_message(self.status_message)
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
        if hasattr(self, 'up_dir') and self.up_dir:
            await clean_download(self.up_dir)
        if self.thumb and await aiopath.exists(self.thumb):
            await remove(self.thumb)