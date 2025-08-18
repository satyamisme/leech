from bot import LOGGER
from ...ext_utils.bot_utils import async_to_sync
from ...ext_utils.files_utils import get_path_size
from ...ext_utils.status_utils import get_readable_file_size
from ...telegram_helper.message_utils import update_status_message

class TelegramDownloadHelper:
    def __init__(self, listener):
        self._client = listener.client
        self._listener = listener
        self._path = ""
        self._total_files = 0
        self._total_folders = 0
        self._folder_name = ""
        self._is_cancelled = False

    async def add_download(self, message, path, folder_name=""):
        try:
            self._path = path
            self._folder_name = folder_name.strip("/")
            if not self._listener.is_recursive:
                self._total_files = 1
                self._total_folders = 0
            else:
                pass

            if await self._download(message, path):
                await self._listener.on_download_complete()
        except Exception as e:
            if not self._is_cancelled:
                await self._listener.on_download_error(str(e))

    async def _download(self, message, path):
        try:
            await self._client.download_media(
                message,
                file_name=path,
                progress=self._progress,
                progress_args=(message,)
            )
            return path
        except Exception as e:
            if not self._is_cancelled:
                LOGGER.error(f"Download error: {e}")
                raise

    async def _progress(self, current, total, message):
        if self._listener.is_cancelled:
            raise Exception("Download cancelled")
        self._listener.downloaded_bytes = current
        self._listener.total_bytes = total
        await self._listener.on_download_progress()

    async def cancel_download(self):
        self._is_cancelled = True
