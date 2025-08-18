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
        self.last_ffmpeg_progress_text = None
        self.kept_indices = set()

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

        from bot.helper.utilities.smart_split import smart_split_if_needed
        from bot.helper.ext_utils.files_utils import get_mime_type
        import os
        from time import strftime

        mime_type = await sync_to_async(get_mime_type, up_path)
        if mime_type.startswith("video/"):
            if self.status_message:
                await edit_message(self.status_message, f"🎬 **Processing Video:** `{self.name}`\n\n⏳ This may take a while...")

            processed_path, self.media_info, self.kept_indices = await process_video(up_path, self)

            if self.is_cancelled: return
            if processed_path:
                up_path = processed_path
                self.name = up_path.replace(f"{self.dir}/", "").split("/", 1)[0]

        if self.is_leech:
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

                base_name = ospath.splitext(self.name)[0].replace(" - Part 001", "")

                uploaded_messages = []
                for i, file_path in enumerate(split_files, 1):
                    tg_uploader._up_path = file_path
                    file_name = ospath.basename(file_path)

                    caption = f"🎬 {base_name}\n\n📁 Part {i} of {len(split_files)}"
                    last_msg = await tg_uploader._upload_file(caption, file_name, file_path)
                    if last_msg:
                        uploaded_messages.append(last_msg)
                    if self.is_cancelled:
                        return

                if self.status_message:
                    await delete_message(self.status_message)

                # After loop, create and send the master summary message
                summary_caption = f"✅ **Upload Complete: {base_name}**\n\n"
                summary_caption += f"📁 **Uploaded Parts ({len(uploaded_messages)}):**\n"

                for msg in uploaded_messages:
                    if msg.document:
                        file_name = msg.document.file_name
                        file_size = msg.document.file_size
                    elif msg.video:
                        file_name = msg.video.file_name
                        file_size = msg.video.file_size
                    else:
                        continue
                    summary_caption += f"**-** [{file_name}]({msg.link}) ({get_readable_file_size(file_size)})\n"

                if self.media_info:
                    total_gb = sum(os.path.getsize(f) for f in split_files) / (1024**3)
                    duration_str = get_readable_time(float(self.media_info['format'].get('duration', 0)))
                    summary_caption += f"\n📊 **Video Info:**"
                    summary_caption += f"\n⏱️ **Duration:** {duration_str}"
                    summary_caption += f"\n📦 **Total Size:** {total_gb:.2f} GB\n"

                    video_streams = [s for s in self.media_info.get('streams', []) if s['codec_type'] == 'video' and s.get('disposition', {}).get('attached_pic') == 0]
                    audio_streams = [s for s in self.media_info.get('streams', []) if s['codec_type'] == 'audio']

                    kept_video = next((s for s in video_streams if s['index'] in self.kept_indices), None)
                    kept_audio = [s for s in audio_streams if s['index'] in self.kept_indices]

                    if kept_video or kept_audio:
                        summary_caption += "\n**Streams Kept:**\n"
                        if kept_video:
                            summary_caption += f"🎥 {self._format_stream_info(kept_video)}\n"
                        for a in kept_audio:
                            summary_caption += f"🔊 {self._format_stream_info(a)}\n"

                await self.message.reply_text(summary_caption, disable_web_page_preview=True, quote=True)
                return

        if self.join:
            await join_files(up_path)

        if self.name_sub:
            up_path = await self.substitute(up_path)
            self.name = up_path.replace(f"{self.dir}/", "").split("/", 1)[0]

        if self.compress:
            up_path = await self.proceed_compress(up_path, gid)
            if self.is_cancelled: return
            self.name = up_path.replace(f"{self.dir}/", "").split("/", 1)[0]

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

            async for sent_message in tg.upload():
                if self.is_cancelled:
                    break
                if sent_message:
                    await self._send_leech_completion_message(sent_message)

            if self.is_cancelled:
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

    def _format_stream_info(self, stream):
        details = []
        codec_type = stream.get('codec_type')
        codec_name = stream.get('codec_name', 'N/A')
        lang = stream.get('tags', {}).get('language', 'und').upper()

        if codec_type == 'video':
            details.append(codec_name)
            if 'profile' in stream:
                details.append(stream['profile'])
            if 'height' in stream:
                details.append(f"{stream['height']}p")
            if 'r_frame_rate' in stream and stream['r_frame_rate'] != '0/0':
                try:
                    num, den = map(int, stream['r_frame_rate'].split('/'))
                    fps = round(num / den, 2)
                    details.append(f"{fps}fps")
                except:
                    details.append(f"{stream['r_frame_rate']}fps")
        elif codec_type == 'audio':
            details.append(codec_name)
            details.append(lang)
            if 'channel_layout' in stream:
                details.append(stream['channel_layout'])
        elif codec_type == 'subtitle':
            details.append(codec_name)
            details.append(lang)

        if stream.get('disposition', {}).get('default'):
            details.append("(Default)")

        return ", ".join(details)

    async def _update_ffmpeg_progress(self):
        if self.status_message is None:
            return
        async with task_dict_lock:
            if self.mid in task_dict:
                task = task_dict[self.mid]
                progress = task.progress()
                text = f"🎬 **Processing Video:** `{self.name}` 🔄\n{get_progress_bar_string(progress)} {progress}"
                if text != self.last_ffmpeg_progress_text:
                    await edit_message(self.status_message, text)
                    self.last_ffmpeg_progress_text = text

    async def _send_leech_completion_message(self, sent_message):
        # This new method only builds and sends the message for a single file.
        name = ospath.basename(sent_message.document.file_name if sent_message.document else sent_message.video.file_name)
        size = sent_message.document.file_size if sent_message.document else sent_message.video.file_size

        summary_caption = f"✅ **Upload Complete: {name}**\n\n"
        summary_caption += f"📁 **Uploaded File:**\n"
        summary_caption += f"**-** [{name}]({sent_message.link}) ({get_readable_file_size(size)})\n"

        if self.media_info:
            duration_str = get_readable_time(float(self.media_info['format'].get('duration', 0)))
            summary_caption += f"\n📊 **Video Info:**\n"
            summary_caption += f"⏱️ **Duration:** {duration_str}\n"

            video_streams = [s for s in self.media_info.get('streams', []) if s['codec_type'] == 'video' and s.get('disposition', {}).get('attached_pic') == 0]
            audio_streams = [s for s in self.media_info.get('streams', []) if s['codec_type'] == 'audio']

            kept_video = next((s for s in video_streams if s['index'] in self.kept_indices), None)
            kept_audio = [s for s in audio_streams if s['index'] in self.kept_indices]

            if kept_video or kept_audio:
                summary_caption += "\n**Streams:**\n"
                if kept_video:
                    summary_caption += f"🎥 {self._format_stream_info(kept_video)}\n"
                for a in kept_audio:
                    summary_caption += f"🔊 {self._format_stream_info(a)}\n"

        summary_caption += f"\n⚡️ @genambot"

        buttons = ButtonMaker()
        buttons.url("View File", sent_message.link)
        await self.message.reply_text(summary_caption, disable_web_page_preview=True, quote=True, reply_markup=buttons.build_menu(1))

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
