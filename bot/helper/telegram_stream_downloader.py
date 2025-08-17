import asyncio
import os
from typing import List
from pyrogram import Client
from pyrogram.errors import FloodWait
from pyrogram.types import Message
from bot import LOGGER

# This class uses pyrogram's `stream_media` method for efficient, single-threaded
# downloads. It is not a true parallel downloader but is faster than the
# default `download_media` method for large files.
class TelegramStreamDownloader:
    def __init__(self, client: Client, message: Message, output_dir: str):
        self.client = client
        self.message = message
        self.output_dir = output_dir
        self.output_path = None
        self.file_id = None
        self.file_size = 0
        self.is_cancelled = False

    async def get_media_info(self):
        media = (
            self.message.video
            or self.message.document
            or self.message.photo
            or None
        )
        if media is None:
            raise ValueError("No media found in message")

        if isinstance(media, list): # handle photos
            media = media[-1]

        self.file_id = media.file_id
        self.file_size = media.file_size
        return media

    def cancel(self):
        """Cancel the download"""
        self.is_cancelled = True
        LOGGER.info("Parallel download cancelled")

    async def download(self):
        try:
            media = await self.get_media_info()

            file_name = getattr(media, 'file_name', f"{self.file_id}.dat")
            self.output_path = os.path.join(self.output_dir, file_name)

            LOGGER.info(f"Starting parallel download to: {self.output_path}")

            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)

            # Pre-allocate file
            with open(self.output_path, "wb") as f:
                f.truncate(self.file_size)

            # Use Pyrogram's built-in stream (fallback to single-threaded)
            async with aiofiles.open(self.output_path, "wb") as f:
                downloaded = 0
                async for chunk in self.client.stream_media(self.message, chunk_size=1024*1024):
                    if self.is_cancelled:
                        break
                    await f.write(chunk)
                    downloaded += len(chunk)
                    if downloaded % 10_000_000 == 0:
                        LOGGER.info(f"Downloaded: {downloaded}/{self.file_size}")

            if self.is_cancelled:
                LOGGER.info("Download cancelled by user.")
                return None

            LOGGER.info(f"Download complete: {self.output_path}")
            return self.output_path

        except Exception as e:
            LOGGER.error(f"Parallel download failed: {e}")
            raise
