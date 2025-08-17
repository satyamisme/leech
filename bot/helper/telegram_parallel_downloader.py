import asyncio
import os
from typing import List
from pyrogram import Client
from pyrogram.errors import FloodWait
from pyrogram.types import Message
from bot import LOGGER

class TelegramParallelDownloader:
    def __init__(self, client: Client, message: Message, output_path: str):
        self.client = client
        self.message = message
        self.output_path = output_path
        self.file_id = None
        self.file_size = 0
        self.is_cancelled = False

    async def get_media_info(self):
        if self.message.video:
            self.file_id = self.message.video.file_id
            self.file_size = self.message.video.file_size
        elif self.message.document:
            self.file_id = self.message.document.file_id
            self.file_size = self.message.document.file_size
        elif self.message.photo:
            self.file_id = self.message.photo[-1].file_id
            self.file_size = self.message.photo[-1].file_size
        else:
            raise ValueError("No media found in message")

    def cancel(self):
        """Cancel the download"""
        self.is_cancelled = True
        LOGGER.info("Parallel download cancelled")

    async def download(self):
        try:
            await self.get_media_info()
            LOGGER.info(f"Starting parallel download: {self.file_id}, Size: {self.file_size}")

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
