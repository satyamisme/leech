from asyncio import create_task, gather, sleep
from functools import partial
from html import escape
from os import path as ospath
from time import time

from aiofiles.os import path as aiopath, remove
from pyrogram.errors import FloodWait, RPCError

from ..common import TaskConfig
from ... import (
    DOWNLOAD_DIR,
    LOGGER,
    non_queued_dl,
    non_queued_up,
    queue_dict_lock,
    task_dict,
    task_dict_lock,
)
from ...core.config_manager import config_dict
from ...core.torrent_manager import TorrentManager
from ..ext_utils.bot_utils import (
    get_readable_file_size,
    get_readable_time,
    sync_to_async,
)
from ..ext_utils.db_handler import database
from ..ext_utils.exceptions import NotSupportedExtractionArchive
from ..ext_utils.files_utils import (
    clean_download,
    clean_target,
    get_base_name,
    get_mime_type,
    is_archive,
    is_first_archive_split,
    is_video,
    join_files,
)
from ..ext_utils.task_manager import start_from_queued
from ..mirror_leech_utils.gdrive_utils.upload import GoogleDriveUpload
from ..mirror_leech_utils.rclone_utils.transfer import RcloneTransferHelper
from ..mirror_leech_utils.status_utils.extract_status import ExtractStatus
from ..mirror_leech_utils.status_utils.gdrive_status import GdriveStatus
from ..mirror_leech_utils.status_utils.rclone_status import RcloneStatus
from ..mirror_leech_utils.status_utils.split_status import SplitStatus
from ..mirror_leech_utils.status_utils.telegram_status import TelegramStatus
from ..mirror_leech_utils.status_utils.unzip_status import UnzipStatus
from ..mirror_leech_utils.status_utils.upload_status import UploadStatus
from ..mirror_leech_utils.status_utils.zip_status import ZipStatus
from ..mirror_leech_utils.telegram_uploader import TelegramUploader
from ..telegram_helper.button_build import ButtonMaker
from ..telegram_helper.message_utils import (
    delete_message,
    edit_message,
    send_message,
    update_status_message,
)
from ..video_utils.processor import process_video


class TaskListener(TaskConfig):
    def __init__(self):
        super().__init__()
        self.streams_kept = None
        self.streams_removed = None
        self.media_info = {}
        self.start_time = time()
        self.total_parts = 1
        self.current_part = 1
        self.is_finished = False
        self.suproc = None
        self.event = None

    async def on_download_start(self):
        if (
            self.is_super_chat
            and config_dict["INCOMPLETE_TASK_NOTIFIER"]
            and self.app is not None
        ):
            await database.add_incomplete_task(
                self.message.chat.id, self.message.link, self.tag
            )

    async def on_download_complete(self):
        if self.is_cancelled or self.is_finished:
            return
        self.is_finished = True
        create_task(
            self._on_download_complete(),
        )

    async def _on_download_complete(self):
        download_path = f"{self.dir}/{self.name}"
        size = 0
        if self.is_leech or self.extract:
            size = await aiopath.getsize(download_path)

        if self.extract and not self.is_leech:
            if is_archive(download_path):
                if is_first_archive_split(download_path):
                    self.name = get_base_name(download_path)
                async with task_dict_lock:
                    task_dict[self.mid] = ExtractStatus(self, size)
                LOGGER.info(f"Extracting: {self.name}")
                self.up_dir = await self._extract(download_path, "")
                if self.is_cancelled:
                    return
                await clean_target(download_path)
            else:
                raise NotSupportedExtractionArchive(
                    f"This extension is not supported to extract: {self.name}"
                )
        elif self.compress and self.is_leech:
            if is_archive(download_path):
                if is_first_archive_split(download_path):
                    self.name = get_base_name(download_path)
                pswd = self.extract_files if self.extract_files else ""
                try:
                    self.up_dir = await self._extract(download_path, pswd)
                except Exception as e:
                    return await self.on_upload_error(str(e))
                if self.is_cancelled:
                    return
                await clean_target(download_path)
            size = await self._get_path_size()
            if self.is_cancelled:
                return
            async with task_dict_lock:
                task_dict[self.mid] = ZipStatus(self, size)
            self.up_path = await self._zip(self.up_dir, "")
            if self.is_cancelled:
                return
            await clean_target(self.up_dir)
        else:
            self.up_dir = download_path

        await self._prepare_and_upload()

    async def _prepare_and_upload(self):
        if self.is_leech:
            if self.is_zip:
                await self._leech(self.up_path, f"{self.name}.zip")
            elif self.equal_splits:
                await self._leech(self.up_dir)
            else:
                await self._leech(self.up_dir, self.name)
        elif self.gdrive_id:
            await self._upload_to_gdrive()
        elif self.rclone_path:
            await self._upload_to_rclone()

    async def _upload(self, up_path, up_name=None):
        if self.is_leech:
            await self._leech(up_path, up_name)
        elif self.gdrive_id:
            await self._upload_to_gdrive()
        elif self.rclone_path:
            await self._upload_to_rclone()

    async def _leech(self, up_path, up_name=None):
        if self.is_cancelled:
            return
        self.up_path = up_path
        if not up_name:
            up_name = self.name
        size = await self._get_path_size()
        if self.is_cancelled:
            return

        if self.equal_splits:
            if self.is_cancelled:
                return
            self.up_path = await self._zip(up_path, "")
            if self.is_cancelled:
                return
            await clean_target(up_path)
            size = await self._get_path_size()
            if self.is_cancelled:
                return

        if self.as_doc and not self.thumb:
            self.thumb = await self._get_thumbnail(up_path)
            if self.is_cancelled:
                return

        if self.is_leech and not self.compress and not self.extract:
            if self.is_cancelled:
                return
            self.up_path, self.media_info = await process_video(up_path, self)
            if self.is_cancelled:
                return

        if not self.name.startswith("From_Total_"):
            self.name = up_name
        else:
            self.name = f"From_Total_{self.name}"

        LOGGER.info(f"Leeching: {self.name}")
        async with task_dict_lock:
            task_dict[self.mid] = TelegramStatus(self, size)
        await self._upload_to_telegram()

    async def _upload_to_telegram(self):
        tg_uploader = TelegramUploader(self)
        tg_sent_messages = await tg_uploader.upload()
        if self.is_cancelled:
            return
        await self.on_upload_complete(None, None, None, None, None, tg_sent_messages)

    async def _upload_to_gdrive(self):
        size = await self._get_path_size()
        if self.is_cancelled:
            return
        LOGGER.info(f"Uploading to Gdrive: {self.name}")
        async with task_dict_lock:
            task_dict[self.mid] = GdriveStatus(self, size)
        gdrive_uploader = GoogleDriveUpload(self)
        link, files, folders, mime_type, dir_id = await gdrive_uploader.upload()
        if self.is_cancelled:
            return
        await self.on_upload_complete(link, files, folders, mime_type, dir_id)

    async def _upload_to_rclone(self):
        size = await self._get_path_size()
        if self.is_cancelled:
            return
        LOGGER.info(f"Uploading to Rclone: {self.name}")
        async with task_dict_lock:
            task_dict[self.mid] = RcloneStatus(self, size)
        rclone_uploader = RcloneTransferHelper(self)
        link, files, folders, mime_type, rclone_path = await rclone_uploader.upload(
            self.up_dir
        )
        if self.is_cancelled:
            return
        await self.on_upload_complete(
            link, files, folders, mime_type, rclone_path, None
        )

    async def _send_leech_completion_message(self, tg_sent_messages):
        """Send rich completion message with real stream info."""
        if not tg_sent_messages:
            return

        msg = ""
        for sent_message in tg_sent_messages:
            if sent_message.document:
                name = sent_message.document.file_name
                size = sent_message.document.file_size
            elif sent_message.video:
                name = sent_message.video.file_name or self.name
                size = sent_message.video.file_size
            else:
                name = self.name
                size = self.size

            msg += f"✅ <b>Task Completed!</b>\n\n"
            msg += f"🎬 <b>Name:</b> <code>{escape(name)}</code>\n"
            msg += f"📦 <b>Size:</b> {get_readable_file_size(size)}\n"
            msg += f"⏱ <b>Elapsed:</b> {get_readable_time(time() - self.start_time)}\n"
            msg += f"👤 <b>User:</b> {self.tag}"

            if self.media_info:
                msg += f"\n\n<b>Media Info:</b>\n"
                msg += f"📺 <b>Resolution:</b> {self.media_info.get('height', 'N/A')}p\n"
                msg += f"🎞 <b>Video Codec:</b> {self.media_info.get('video_codec', 'N/A').upper()}\n"
                msg += f"🗣 <b>Audio:</b> {self.media_info.get('audio_lang', 'N/A').upper()}"

        buttons = ButtonMaker()
        if link:
            buttons.url_button("Cloud Link", link)
        reply_markup = buttons.build_menu(2) if buttons._button else None

        await send_message(self.message, msg, reply_markup)

    async def on_upload_complete(
        self, link, files, folders, mime_type, rclone_path="", dir_id="", tg_sent_messages=None
    ):
        if (
            self.is_super_chat
            and config_dict["INCOMPLETE_TASK_NOTIFIER"]
            and self.app is not None
        ):
            await database.rm_complete_task(self.message.link)

        if self.is_leech:
            await self._send_leech_completion_message(tg_sent_messages)
        else:
            msg = f"✅ <b>Task Completed!</b>\n\n"
            msg += f"🎬 <b>Name:</b> <code>{escape(self.name)}</code>\n"
            msg += f"📦 <b>Size:</b> {get_readable_file_size(self.size)}\n"
            msg += f"⏱ <b>Elapsed:</b> {get_readable_time(time() - self.start_time)}\n"
            msg += f"👤 <b>User:</b> {self.tag}"

            buttons = ButtonMaker()
            if link:
                buttons.url_button("Cloud Link", link)
            reply_markup = buttons.build_menu(2) if buttons._button else None

            await send_message(self.message, msg, reply_markup)

        if self.seed:
            await clean_target(self.up_dir)
        else:
            await clean_download(self.dir)

        await self._on_task_complete()

    async def on_upload_error(self, error):
        await send_message(self.message, f"❌ {escape(str(error))}")
        await self._on_task_complete()

    async def _on_task_complete(self):
        self.is_cancelled = True
        async with task_dict_lock:
            if self.mid in task_dict:
                del task_dict[self.mid]
            count = len(task_dict)

        async with queue_dict_lock:
            if self.mid in non_queued_up:
                non_queued_up.remove(self.mid)
            if self.mid in non_queued_dl:
                non_queued_dl.remove(self.mid)

        if count == 0:
            await self._clean()
        else:
            await update_status_message(self.message.chat.id, True)

        if (
            self.is_super_chat
            and config_dict["INCOMPLETE_TASK_NOTIFIER"]
            and self.app is not None
        ):
            await database.rm_complete_task(self.message.link)

        if self.is_leech:
            await clean_target(self.up_path)

    async def _clean(self):
        try:
            await gather(
                delete_message(self.status_message),
                TorrentManager().aria2.purge_all(),
            )
        except:
            pass
