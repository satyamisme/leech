import asyncio
import os
import math
from pyrogram import Client
from pyrogram.raw import functions, types
from pyrogram.raw.types import InputFileLocation
from pyrogram.errors import FloodWait, RPCError
import aiofiles
import logging
from bot.core.config_manager import Config

# Configure logging
logger = logging.getLogger(__name__)

class TelegramParallelDownloader:
    def __init__(self, client: Client, message, path, listener):
        self.client = client
        self.message = message
        self.path = path
        self.listener = listener
        self.is_cancelled = False
        self.downloaded_bytes = 0
        self.last_progress_update = 0
        self.start_time = asyncio.get_event_loop().time()
        self.download_speed = 0
        self.file_size = 0

    async def get_file_info(self):
        """Fetch media info (size, file_id) from message"""
        media = (
            self.message.document
            or self.message.photo
            or self.message.video
            or self.message.audio
            or self.message.voice
            or self.message.video_note
            or self.message.sticker
            or self.message.animation
            or None
        )
        if media is None:
            raise ValueError("Message has no media")

        self.file_size = media.file_size
        return media

    async def get_file_location(self, media):
        """Get InputFileLocation for raw MTProto calls"""
        if isinstance(media, types.MessageMediaDocument):
            return types.InputDocumentFileLocation(
                id=media.document.id,
                access_hash=media.document.access_hash,
                file_reference=media.document.file_reference,
                thumb_size=-1
            )
        elif isinstance(media, types.MessageMediaPhoto):
            return types.InputPhotoFileLocation(
                id=media.photo.id,
                access_hash=media.photo.access_hash,
                file_reference=media.photo.file_reference,
                thumb_size=-1
            )
        elif isinstance(media, types.MessageMediaVideo):
            return types.InputVideoFileLocation(
                id=media.video.id,
                access_hash=media.video.access_hash,
                file_reference=media.video.file_reference,
                thumb_size=-1
            )
        else:
            raise TypeError("Unsupported media type")

    async def download_part(
        self,
        file_loc: InputFileLocation,
        part_idx: int,
        offset: int,
        limit: int
    ):
        """Download a single part of the file"""
        retries = 5
        for attempt in range(retries):
            if self.is_cancelled:
                return

            try:
                data = await self.client.invoke(
                    functions.upload.GetFile(
                        location=file_loc,
                        offset=offset,
                        limit=limit,
                        precise=True
                    )
                )

                if not hasattr(data, "bytes") or len(data.bytes) == 0:
                    return  # Empty response

                # Write part directly to correct offset
                async with aiofiles.open(self.path, "r+b") as f:
                    await f.seek(offset)
                    await f.write(data.bytes)

                # Update progress
                self.update_progress(len(data.bytes))

                logger.info(f"Part {part_idx} downloaded: {offset} → {offset + len(data.bytes)}")
                return

            except FloodWait as fw:
                logger.warning(f"FloodWait: Sleeping {fw.value} seconds...")
                await asyncio.sleep(fw.value)
            except RPCError as e:
                logger.error(f"RPCError on part {part_idx}: {e}, retry {attempt + 1}/{retries}")
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
            except Exception as e:
                logger.error(f"Error downloading part {part_idx}: {e}")
                return

        logger.error(f"Failed to download part {part_idx} after {retries} attempts")

    def update_progress(self, chunk_size):
        """Update download progress efficiently"""
        self.downloaded_bytes += chunk_size

        # Throttle progress updates
        current_time = asyncio.get_event_loop().time()
        time_diff = current_time - self.last_progress_update

        # Update at least every 1 second or 5MB
        if time_diff > 1.0 or (self.downloaded_bytes - self.last_progress_update) > 5 * 1024 * 1024:
            elapsed = current_time - self.start_time
            self.download_speed = self.downloaded_bytes / elapsed if elapsed > 0 else 0

            # Update listener with progress
            if self.listener:
                self.listener.onDownloadProgress(
                    self.downloaded_bytes,
                    self.file_size,
                    self.download_speed
                )

            self.last_progress_update = self.downloaded_bytes

    async def download(self):
        """Main download method with parallel execution"""
        try:
            logger.info("Starting parallel Telegram download")
            media = await self.get_file_info()
            file_loc = await self.get_file_location(media)

            # Pre-allocate file
            async with aiofiles.open(self.path, "wb") as f:
                await f.truncate(self.file_size)

            # Calculate parts
            chunk_size_bytes = Config.CHUNK_SIZE * 1024 * 1024
            num_parts = min(math.ceil(self.file_size / chunk_size_bytes), Config.MAX_PARALLEL_CHUNKS)
            part_size = math.ceil(self.file_size / num_parts)

            logger.info(f"Splitting into {num_parts} parts, ~{part_size//1024//1024} MB each")

            # Create download tasks
            tasks = []
            for i in range(num_parts):
                offset = i * part_size
                limit = min(part_size, self.file_size - offset)
                tasks.append(
                    self.download_part(file_loc, i, offset, limit)
                )

            # Run tasks in parallel
            await asyncio.gather(*tasks)

            # Verify download
            downloaded_size = os.path.getsize(self.path)
            if downloaded_size == self.file_size:
                logger.info(f"Download complete: {self.path}")
                return True
            else:
                missing = self.file_size - downloaded_size
                logger.error(f"Incomplete download. Missing {missing} bytes.")
                return False

        except Exception as e:
            logger.error(f"Parallel download failed: {str(e)}")
            return False

    def cancel(self):
        """Cancel the download"""
        self.is_cancelled = True
        logger.info("Parallel download cancelled")
