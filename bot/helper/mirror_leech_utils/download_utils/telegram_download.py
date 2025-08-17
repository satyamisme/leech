from asyncio import Lock, sleep, Semaphore, gather
from time import time
import math
import aiofiles
from pyrogram.errors import FloodWait, FloodPremiumWait

from .... import (
    LOGGER,
    task_dict,
    task_dict_lock,
)
from ....core.mltb_client import TgClient
from ...ext_utils.task_manager import check_running_tasks, stop_duplicate_check
from ...mirror_leech_utils.status_utils.queue_status import QueueStatus
from ...mirror_leech_utils.status_utils.telegram_status import TelegramStatus
from ...telegram_helper.message_utils import send_status_message
from ...ext_utils.bot_utils import calculate_dynamic_chunk_size

global_lock = Lock()
GLOBAL_GID = set()

class TelegramDownloadHelper:
    def __init__(self, listener):
        self._processed_bytes = 0
        self._start_time = time()
        self._listener = listener
        self._id = ""
        self.session = ""
        self._accumulated = 0
        self._last_reported = 0
        self._update_threshold = 5 * 1024 * 1024  # 5MB

    @property
    def speed(self):
        return self._processed_bytes / (time() - self._start_time)

    @property
    def processed_bytes(self):
        return self._processed_bytes

    async def _on_download_start(self, file_id, from_queue):
        async with global_lock:
            GLOBAL_GID.add(file_id)
        self._id = file_id
        async with task_dict_lock:
            task_dict[self._listener.mid] = TelegramStatus(
                self._listener, self, file_id[:12], "dl"
            )
        if not from_queue:
            await self._listener.on_download_start()
            if self._listener.multi <= 1:
                await send_status_message(self._listener.message)
            LOGGER.info(f"Download from Telegram: {self._listener.name}")
        else:
            LOGGER.info(f"Start Queued Download from Telegram: {self._listener.name}")

    async def _update_progress(self, size):
        self._accumulated += size
        if self._accumulated >= self._update_threshold:
            self._processed_bytes += self._accumulated
            self._accumulated = 0

    async def _on_download_error(self, error):
        async with global_lock:
            if self._id in GLOBAL_GID:
                GLOBAL_GID.remove(self._id)
        await self._listener.on_download_error(error)

    async def _on_download_complete(self):
        async with global_lock:
            if self._id in GLOBAL_GID:
                GLOBAL_GID.remove(self._id)
        await self._listener.on_download_complete()

    async def _download_part(self, client, media, path, start, end, semaphore):
        CHUNK_SIZE = 1024 * 1024  # Telegram chunk size
        async with semaphore:
            chunk_offset = start // CHUNK_SIZE
            num_chunks = ((end - start + 1) + CHUNK_SIZE - 1) // CHUNK_SIZE
            try:
                async with aiofiles.open(path, 'r+b') as f:
                    await f.seek(start)
                    transferred = 0
                    async for chunk in client.stream_media(media, offset=chunk_offset, limit=num_chunks):
                        if self._listener.is_cancelled:
                            if self.session == "user":
                                TgClient.user.stop_transmission()
                            else:
                                TgClient.bot.stop_transmission()
                            return
                        await f.write(chunk)
                        await self._update_progress(len(chunk))
                        transferred += len(chunk)
            except (FloodWait, FloodPremiumWait) as f:
                LOGGER.warning(str(f))
                await sleep(f.value)
                await self._download_part(client, media, path, start, end, semaphore)
            except Exception as e:
                LOGGER.error(str(e))
                await self._on_download_error(str(e))

    async def _download(self, message, path):
        media = (
            message.document
            or message.photo
            or message.video
            or message.audio
            or message.voice
            or message.video_note
            or message.sticker
            or message.animation
            or None
        )
        if media is None:
            await self._on_download_error("No media to download")
            return

        client = TgClient.user if self.session == "user" else TgClient.bot
        file_size = self._listener.size

        try:
            # Pre-allocate file space
            async with aiofiles.open(path, 'wb') as f:
                await f.truncate(file_size)

            part_size = calculate_dynamic_chunk_size(file_size)
            parts = []
            start = 0
            while start < file_size:
                end = min(start + part_size - 1, file_size - 1)
                parts.append((start, end))
                start = end + 1

            MAX_PARALLEL = 20
            max_concurrency = min(MAX_PARALLEL, max(10, 1000 // (len(parts) // 100 + 1)))
            semaphore = Semaphore(max_concurrency)

            tasks = [self._download_part(client, media, path, s, e, semaphore) for s, e in parts]

            BATCH_SIZE = 50
            if len(tasks) > BATCH_SIZE:
                for i in range(0, len(tasks), BATCH_SIZE):
                    await gather(*tasks[i:i + BATCH_SIZE])
                    await sleep(0.1)
            else:
                await gather(*tasks)

            if self._listener.is_cancelled:
                return
            await self._on_download_complete()
        except Exception as e:
            LOGGER.error(str(e))
            await self._on_download_error(str(e))
            return

    async def add_download(self, message, path, session):
        self.session = session
        if not self.session:
            if self._listener.user_transmission and self._listener.is_super_chat:
                self.session = "user"
                message = await TgClient.user.get_messages(
                    chat_id=message.chat.id, message_ids=message.id
                )
            else:
                self.session = "bot"
        media = (
            message.document
            or message.photo
            or message.video
            or message.audio
            or message.voice
            or message.video_note
            or message.sticker
            or message.animation
            or None
        )

        if media is not None:
            self._listener.is_file = True
            async with global_lock:
                download = media.file_unique_id not in GLOBAL_GID

            if download:
                if not self._listener.name:
                    if hasattr(media, "file_name") and media.file_name:
                        if "/" in media.file_name:
                            self._listener.name = media.file_name.rsplit("/", 1)[-1]
                            path = path + self._listener.name
                        else:
                            self._listener.name = media.file_name
                    else:
                        self._listener.name = "None"
                else:
                    path = path + self._listener.name
                self._listener.size = media.file_size
                gid = media.file_unique_id

                msg, button = await stop_duplicate_check(self._listener)
                if msg:
                    await self._listener.on_download_error(msg, button)
                    return

                add_to_queue, event = await check_running_tasks(self._listener)
                if add_to_queue:
                    LOGGER.info(f"Added to Queue/Download: {self._listener.name}")
                    async with task_dict_lock:
                        task_dict[self._listener.mid] = QueueStatus(
                            self._listener, gid, "dl"
                        )
                    await self._listener.on_download_start()
                    if self._listener.multi <= 1:
                        await send_status_message(self._listener.message)
                    await event.wait()
                    if self.session == "bot":
                        message = await self._listener.client.get_messages(
                            chat_id=message.chat.id, message_ids=message.id
                        )
                    else:
                        message = await TgClient.user.get_messages(
                            chat_id=message.chat.id, message_ids=message.id
                        )
                    if self._listener.is_cancelled:
                        async with global_lock:
                            if self._id in GLOBAL_GID:
                                GLOBAL_GID.remove(self._id)
                        return
                self._start_time = time()
                await self._on_download_start(gid, add_to_queue)
                await self._download(message, path)
            else:
                await self._on_download_error("File already being downloaded!")
        else:
            await self._on_download_error(
                "No document in the replied message! Use SuperGroup incase you are trying to download with User session!"
            )

    async def cancel_task(self):
        self._listener.is_cancelled = True
        LOGGER.info(
            f"Cancelling download on user request: name: {self._listener.name} id: {self._id}"
        )
        await self._on_download_error("Stopped by user!")
