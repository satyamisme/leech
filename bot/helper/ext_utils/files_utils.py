from aioshutil import rmtree as aiormtree, move
from asyncio import (
    create_subprocess_exec,
    sleep,
    wait_for,
    gather,
    create_subprocess_shell,
)
from asyncio.subprocess import PIPE
from magic import Magic
from os import walk, path as ospath, readlink
from re import split as re_split, I, search as re_search, escape
from aiofiles.os import (
    remove,
    path as aiopath,
    listdir,
    rmdir,
    readlink as aioreadlink,
    symlink,
    makedirs as aiomakedirs,
)
from pyrogram.enums import ChatType

from ... import LOGGER, DOWNLOAD_DIR
from ...core.torrent_manager import TorrentManager
from .bot_utils import sync_to_async, cmd_exec
from .exceptions import NotSupportedExtractionArchive

ARCH_EXT = [
    ".tar.bz2",
    ".tar.gz",
    ".bz2",
    ".gz",
    ".tar.xz",
    ".tar",
    ".tbz2",
    ".tgz",
    ".lzma2",
    ".zip",
    ".7z",
    ".z",
    ".rar",
    ".iso",
    ".wim",
    ".cab",
    ".apm",
    ".arj",
    ".chm",
    ".cpio",
    ".cramfs",
    ".deb",
    ".dmg",
    ".fat",
    ".hfs",
    ".lzh",
    ".lzma",
    ".mbr",
    ".msi",
    ".mslz",
    ".nsis",
    ".ntfs",
    ".rpm",
    ".squashfs",
    ".udf",
    ".vhd",
    ".xar",
    ".zst",
    ".zstd",
    ".cbz",
    ".apfs",
    ".ar",
    ".qcow",
    ".macho",
    ".exe",
    ".dll",
    ".sys",
    ".pmd",
    ".swf",
    ".swfc",
    ".simg",
    ".vdi",
    ".vhdx",
    ".vmdk",
    ".gzip",
    ".lzma86",
    ".sha256",
    ".sha512",
    ".sha224",
    ".sha384",
    ".sha1",
    ".md5",
    ".crc32",
    ".crc64",
]


FIRST_SPLIT_REGEX = (
    r"\.part0*1\.rar$|\.7z\.0*1$|\.zip\.0*1$|^(?!.*\.part\d+\.rar$).*\.rar$"
)

SPLIT_REGEX = r"\.r\d+$|\.7z\.\d+$|\.z\d+$|\.zip\.\d+$|\.part\d+\.rar$"


def is_first_archive_split(file):
    return bool(re_search(FIRST_SPLIT_REGEX, file.lower(), I))


def is_archive(file):
    return file.strip().lower().endswith(tuple(ARCH_EXT))


def is_archive_split(file):
    return bool(re_search(SPLIT_REGEX, file.lower(), I))


async def clean_target(opath):
    if await aiopath.exists(opath):
        LOGGER.info(f"Cleaning Target: {opath}")
        try:
            if await aiopath.isdir(opath):
                await aiormtree(opath, ignore_errors=True)
            else:
                await remove(opath)
        except Exception as e:
            LOGGER.error(str(e))


async def clean_download(opath):
    if not await aiopath.exists(opath):
        return
    LOGGER.info(f"Cleaning Download: {opath}")
    try:
        await aiormtree(opath, ignore_errors=True)
    except Exception as e:
        LOGGER.error(str(e))


async def clean_all():
    await TorrentManager.remove_all()
    LOGGER.info("Cleaning Download Directory")
    await (await create_subprocess_exec("rm", "-rf", DOWNLOAD_DIR)).wait()
    await aiomakedirs(DOWNLOAD_DIR, exist_ok=True)


async def clean_unwanted(opath):
    LOGGER.info(f"Cleaning unwanted files/folders: {opath}")
    for dirpath, _, files in await sync_to_async(walk, opath, topdown=False):
        for filee in files:
            f_path = ospath.join(dirpath, filee)
            if filee.strip().endswith(".parts") and filee.startswith("."):
                await remove(f_path)
        if dirpath.strip().endswith(".unwanted"):
            await aiormtree(dirpath, ignore_errors=True)
    for dirpath, _, files in await sync_to_async(walk, opath, topdown=False):
        if not await listdir(dirpath):
            await rmdir(dirpath)


async def get_path_size(path):
    if await aiopath.isfile(path):
        if await aiopath.islink(path):
            link_path = await aioreadlink(path)
            return await aiopath.getsize(link_path)
        return await aiopath.getsize(path)
    total_size = 0
    async for root, _, files in aiopath.walk(path):
        for f in files:
            fp = ospath.join(root, f)
            if await aiopath.islink(fp):
                continue
            total_size += await aiopath.getsize(fp)
    return total_size


async def count_files_and_folders(opath):
    if not await aiopath.exists(opath):
        return 0, 0
    total_files = sum(len(files) for _, _, files in await sync_to_async(walk, opath))
    total_folders = sum(
        len(dirs) for _, dirs, _ in await sync_to_async(walk, opath)
    )
    return total_folders, total_files


def get_base_name(orig_path):
    extension = next(
        (ext for ext in ARCH_EXT if orig_path.strip().lower().endswith(ext)), ""
    )
    if extension != "":
        return re_split(f"{extension}$", orig_path, maxsplit=1, flags=I)[0]
    else:
        raise NotSupportedExtractionArchive("File format not supported for extraction")


async def create_recursive_symlink(source, destination):
    if ospath.isdir(source):
        await aiomakedirs(destination, exist_ok=True)
        for item in await listdir(source):
            item_source = ospath.join(source, item)
            item_dest = ospath.join(destination, item)
            await create_recursive_symlink(item_source, item_dest)
    elif ospath.isfile(source):
        try:
            await symlink(source, destination)
        except FileExistsError:
            LOGGER.error(f"Shortcut already exists: {destination}")
        except Exception as e:
            LOGGER.error(f"Error creating shortcut for {source}: {e}")


def get_mime_type(file_path):
    try:
        mime = Magic(mime=True)
        if ospath.isdir(file_path):
            return "application/x-directory"
        mime_type = mime.from_file(file_path)
        return mime_type or "application/octet-stream"
    except Exception as e:
        LOGGER.warning(f"Get mime type: {e}")
        return "application/octet-stream"


async def remove_excluded_files(fpath, ee):
    for root, _, files in await sync_to_async(walk, fpath):
        for f in files:
            if f.strip().lower().endswith(tuple(ee)):
                await remove(ospath.join(root, f))


async def move_and_merge(source, destination, mid):
    try:
        if not await aiopath.exists(destination):
            await aiomakedirs(destination, exist_ok=True)

        for item in await listdir(source):
            src_path = ospath.join(source, item)
            dest_path = ospath.join(destination, item)

            if await aiopath.isdir(src_path):
                if await aiopath.exists(dest_path):
                    await move_and_merge(src_path, dest_path, mid)
                else:
                    await move(src_path, dest_path)
            else:
                if item.endswith((".aria2", ".!qB")):
                    continue
                if await aiopath.exists(dest_path):
                    dest_path = ospath.join(destination, f"{mid}-{item}")
                await move(src_path, dest_path)
    except Exception as e:
        LOGGER.error(f"Error while moving and merging: {e}")


async def join_files(opath):
    if not await aiopath.isdir(opath):
        LOGGER.error(f"Path not found: {opath}")
        return
    try:
        files = await listdir(opath)
        first_part = next(
            (
                f
                for f in files
                if re_search(r"\.0*1$|\.part0*1\.rar$", f, re_IGNORECASE)
            ),
            None,
        )
        if not first_part:
            LOGGER.warning("No split archives found to join.")
            return

        first_part_path = ospath.join(opath, first_part)
        LOGGER.info(f"Joining split archive: {first_part}")

        pswd = ""
        if match := re_search(r"pass(?:word)?=([^\s]+)", first_part, re_IGNORECASE):
            pswd = match[1]

        cmd = [
            "7z",
            "x",
            f"-p{pswd}" if pswd else "",
            "-y",
            f"-o{opath}",
            first_part_path,
        ]
        if not pswd:
            cmd.pop(2)

        process = await create_subprocess_shell(" ".join(cmd), stderr=PIPE)
        _, stderr = await process.communicate()
        code = process.returncode

        if code == 0:
            LOGGER.info("Successfully joined split archive.")
            # Clean up split files
            await gather(
                *(
                    remove(ospath.join(opath, f))
                    for f in files
                    if re_search(r"\.\d+$|\.part\d+\.rar$", f, re_IGNORECASE)
                )
            )
        else:
            LOGGER.error(f"Failed to join split archive: {stderr.decode().strip()}")
    except Exception as e:
        LOGGER.error(f"An error occurred while joining files: {e}")


async def split_file(f_path, split_size, listener):
    out_path = f"{f_path}.part"
    if listener.is_cancelled:
        return False
    listener.subproc = await create_subprocess_exec(
        "split",
        "--numeric-suffixes=1",
        "--suffix-length=2",
        f"--bytes={split_size}",
        f_path,
        out_path,
        ".split.mkv",
        stderr=PIPE,
    )
    _, stderr = await listener.subproc.communicate()
    code = listener.subproc.returncode
    if listener.is_cancelled:
        return False
    if code == -9:
        listener.is_cancelled = True
        return False
    elif code != 0:
        try:
            stderr = stderr.decode().strip()
        except:
            stderr = "Unable to decode the error!"
        LOGGER.error(f"{stderr}. Split Document: {f_path}")
    return True


async def is_video(file_path):
    try:
        if not await aiopath.isfile(file_path):
            return False
        mime_type = await sync_to_async(get_mime_type, file_path)
        if mime_type.startswith("video"):
            return True
        process = await create_subprocess_exec(
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=start_time,duration",
            "-of",
            "csv=p=0",
            file_path,
        )
        return (await process.wait()) == 0
    except Exception:
        return False


class SevenZ:
    def __init__(self, listener):
        self._listener = listener
        self._processed_bytes = 0
        self._percentage = "0%"

    @property
    def processed_bytes(self):
        return self._processed_bytes

    @property
    def progress(self):
        return self._percentage

    async def _sevenz_progress(self):
        pattern = r"(\d+)\s+b"
        while not self._listener.is_cancelled and self._listener.subproc:
            try:
                line = await wait_for(self._listener.subproc.stdout.readline(), 2)
            except:
                break
            line = line.decode().strip()
            if match := re_search(pattern, line):
                self._listener.subsize = int(match[1] or match[2])

        while not self._listener.is_cancelled and self._listener.subproc:
            try:
                char = await wait_for(self._listener.subproc.stdout.read(1), 60)
            except:
                break
            if not char:
                break
            if char.isspace():
                continue
            self._processed_bytes += 1
            try:
                self._percentage = f"{self._processed_bytes / self._listener.subsize * 100:.2f}%"
            except:
                pass
            finally:
                await sleep(0.05)

    async def extract(self, f_path, t_path, pswd):
        cmd = [
            "7z",
            "x",
            f"-p{pswd}",
            f_path,
            f"-o{t_path}",
            "-aot",
            "-xr!@PaxHeader",
            "-bsp1",
            "-bse1",
            "-bb3",
        ]
        if not pswd or self._listener.message.chat.type == ChatType.PRIVATE:
            del cmd[2]
        else:
            cmd[2] = f"-p{pswd}"

        self._listener.subproc = await create_subprocess_exec(
            *cmd,
            stdout=PIPE,
            stderr=PIPE,
        )
        await self._sevenz_progress()
        _, stderr = await self._listener.subproc.communicate()
        code = self._listener.subproc.returncode

        if self._listener.is_cancelled:
            return False
        elif code == -9:
            self._listener.is_cancelled = True
            return False
        elif code != 0:
            try:
                stderr = stderr.decode().strip()
            except:
                stderr = "Unable to decode error!"
            LOGGER.error(f"{stderr}. Unable to extract archive!. Path: {f_path}")
            return False
        return True

    async def zip(self, dl_path, up_path, pswd):
        size = await get_path_size(dl_path)
        if self._listener.equal_splits:
            parts = -(-size // self._listener.split_size)
            split_size = (size // parts) + (size % parts)
        else:
            split_size = self._listener.split_size

        cmd = [
            "7z",
            f"-v{split_size}b",
            "a",
            "-mx=0",
            f"-p{pswd}",
            up_path,
            dl_path,
            "-bsp1",
            "-bse1",
            "-bb3",
        ]

        if self._listener.is_leech and int(size) > self._listener.split_size:
            if not pswd or self._listener.message.chat.type == ChatType.PRIVATE:
                cmd.pop(4)
            LOGGER.info(f"Zip: orig_path: {dl_path}, zip_path: {up_path}.0*")
        else:
            cmd.pop(1)
            if not pswd or self._listener.message.chat.type == ChatType.PRIVATE:
                cmd.pop(3)
            LOGGER.info(f"Zip: orig_path: {dl_path}, zip_path: {up_path}")

        self._listener.subproc = await create_subprocess_exec(
            *cmd, stdout=PIPE, stderr=PIPE
        )
        await self._sevenz_progress()
        _, stderr = await self._listener.subproc.communicate()
        code = self._listener.subproc.returncode
        if self._listener.is_cancelled:
            return dl_path
        elif code == -9:
            self._listener.is_cancelled = True
            return dl_path
        elif code == 0:
            await clean_target(dl_path)
            return up_path
        else:
            if await aiopath.exists(up_path):
                await remove(up_path)
            try:
                stderr = stderr.decode().strip()
            except:
                stderr = "Unable to decode error!"
            LOGGER.error(f"{stderr}. Unable to zip this path: {dl_dl_path}")
            return dl_path
