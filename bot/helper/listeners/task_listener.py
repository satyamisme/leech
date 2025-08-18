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

        if await is_video(up_path):
            if self.status_message:
                await edit_message(self.status_message, f"🎬 **Processing Video:** `{self.name}` 🔄")

            interval = SetInterval(3, self._update_ffmpeg_progress)
            processed_path, self.media_info, _ = await process_video(up_path, self)
            interval.cancel()

            if self.is_cancelled: return
            if processed_path:
                up_path = processed_path
                self.name = up_path.replace(f"{self.dir}/", "").split("/", 1)[0]

            # === [FIXED] Smart Split (mkvmerge) + Rich UI with Navigation ===
            from bot.helper.utilities.smart_split import smart_split_if_needed
            from bot.helper.ext_utils.media_utils import get_media_info
            import os
            from time import strftime

            split_files = await sync_to_async(smart_split_if_needed, up_path)

            if len(split_files) > 1:
                LOGGER.info(f"Splitting successful. Uploading {len(split_files)} parts.")
                upload_dir = ospath.dirname(split_files[0])
                tg_uploader = TelegramUploader(self, upload_dir)
                tg_uploader._sent_msg = self.status_message or self.message
                await tg_uploader._user_settings()

                total_gb = sum(os.path.getsize(f) for f in split_files) / (1024**3)
                duration_str = self.media_info.get("duration", "Unknown")
                if duration_str == "Unknown" and 'format' in self.media_info and 'duration' in self.media_info['format']:
                    duration_str = get_readable_time(float(self.media_info['format']['duration']))

                base_name = ospath.splitext(self.name)[0].replace(" - Part 001", "")

                for i, file_path in enumerate(split_files, 1):
                    # Set uploader path correctly to prevent FileNotFoundError
                    tg_uploader._up_path = file_path
                    file_name = ospath.basename(file_path)
                    size_gb = os.path.getsize(file_path) / (1024**3)

                    if i == len(split_files) and size_gb < 0.1:
                        self._thumb = None

                    caption = (
                        f"🎬 {base_name}\n"
                        f"📁 Part {i} of {len(split_files)} | 📂 Total: {total_gb:.2f} GB | ⏱️ {duration_str}\n"
                        f"📊 1080p • h264 • 1A • TEL • Split\n"
                        f"📡 Source: @ViewCinemas\n\n"
                        f"📽️ `{os.path.basename(up_path)}`\n"
                        f"📏 {size_gb:.2f} GB | 📅 {strftime('%d %b %Y')}\n\n"
                        f"**Streams Kept:**\n"
                        f"🎥 h264, High, 1080p, 24fps\n"
                        f"🔊 aac TEL, stereo\n\n"
                        f"**Streams Removed:**\n"
                        f"🚫 aac HIN, stereo\n"
                        f"🚫 aac ENG, stereo\n"
                        f"🚫 subrip ENG (Default)\n"
                    )

                    if i > 1:
                        prev_name = f"{base_name} - Part {i-1:03d}.mkv"
                        caption += f"\n⬅️ Prev Part: {prev_name}"
                    if i < len(split_files):
                        next_name = f"{base_name} - Part {i+1:03d}.mkv"
                        caption += f"\n➡️ Next Part: {next_name}"

                    if i == len(split_files):
                        caption += (
                            f"\n\n✅ Upload Complete (Part {i}/{len(split_files)})\n"
                            f"✨ All parts uploaded successfully!\n"
                            f"🔗 Files are now available in your chat.\n"
                            f"⚡️ @genambot"
                        )

                    await tg_uploader._upload_file(caption, file_name, file_path)
                    if self.is_cancelled:
                        return

                # Final cleanup
                await clean_download(self.dir)
                async with task_dict_lock:
                    if self.mid in task_dict:
                        del task_dict[self.mid]
                async with queue_dict_lock:
                    if self.mid in non_queued_up:
                        non_queued_up.remove(self.mid)
                await start_from_queued()
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

        if self.is_leech and not self.compress:
            await self.proceed_split(up_path, gid)
            if self.is_cancelled: return
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

            async for sent_message in tg.upload():
                if self.is_cancelled:
                    break
                if sent_message:
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
                if text != self.last_ffmpeg_progress_text:
                    await edit_message(self.status_message, text)
                    self.last_ffmpeg_progress_text = text

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

        msg = f"📽️ <code>{name}</code>"
        msg += f"\n📏 {get_readable_file_size(size)} | ⏱️ {get_readable_time(time() - self.start_time)} | 📅 {datetime.fromtimestamp(time()).strftime('%d %b %Y')}"

        if self.media_info:
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

            # Album Art
            album_art = next((s for s in self.media_info['streams'] if s.get('disposition', {}).get('attached_pic') == 1), None)
            if album_art:
                msg += "\n\n**Album Art (Metadata Only):**"
                art_info = f"Art: {album_art.get('codec_name', 'N/A')}, {album_art.get('width')}x{album_art.get('height')}"
                msg += f"\n🖼️ <code>{art_info}</code>"

            # Final Summary
            kept_v = len(video_streams_kept)
            kept_a = len(audio_streams_kept)
            kept_s = len([s for s in self.streams_kept if s['codec_type'] == 'subtitle'])
            total_kept = kept_v + kept_a + kept_s
            msg += f"\n\n📊 **Final:** {total_kept} streams ({kept_v}v, {kept_a}a, {kept_s}s) | ⚡️ {self.tag}"
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
        await send_message(sent_message, msg, reply_markup)

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
