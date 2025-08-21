from aioshutil import rmtree as aiormtree, move
from asyncio import create_subprocess_exec, sleep, wait_for, gather
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
    if await aiopath.exists(opath):
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
    total_size = 0
    if await aiopath.isfile(path):
        if await aiopath.islink(path):
            path = await aioreadlink(path)
        return await aiopath.getsize(path)
    async for root, _, files in aiopath.walk(path):
        for f in files:
            fp = ospath.join(root, f)
            if await aiopath.islink(fp):
                continue
            total_size += await aiopath.getsize(fp)
    return total_size


async def count_files_and_folders(opath):
    total_files = 0
    total_folders = 0
    for _, dirs, files in await sync_to_async(walk, opath):
        total_files += len(files)
        total_folders += len(dirs)
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
    mime = Magic(mime=True)
    if ospath.isdir(file_path):
        return 'application/x-directory'
    mime_type = mime.from_file(file_path)
    return mime_type or 'application/octet-stream'


async def remove_excluded_files(fpath, ee):
    for root, _, files in await sync_to_async(walk, fpath):
        for f in files:
            if f.strip().lower().endswith(tuple(ee)):
                await remove(ospath.join(root, f))


async def move_and_merge(source, destination, mid):
    if not await aiopath.exists(destination):
        await aiomakedirs(destination, exist_ok=True)
    for item in await listdir(source):
        item = item.strip()
        src_path = f"{source}/{item}"
        dest_path = f"{destination}/{item}"
        if await aiopath.isdir(src_path):
            if await aiopath.exists(dest_path):
                await move_and_merge(src_path, dest_path, mid)
            else:
                await move(src_path, dest_path)
        else:
            if item.endswith((".aria2", ".!qB")):
                continue
            if await aiopath.exists(dest_path):
                dest_path = f"{destination}/{mid}-{item}"
            await move(src_path, dest_path)


async def join_files(opath):
    """Use 7z to join split archives (.001, .part1.rar, etc.)"""
    files = await listdir(opath)
    first_part = None
    for file in files:
        if re_search(r'\.0*1$', file) or file.endswith('.part1.rar'):
            first_part = ospath.join(opath, file)
            break
    if not first_part:
        return
    mime_type = await sync_to_async(get_mime_type, first_part)
    if mime_type in ['application/x-7z-compressed', 'application/zip']:
        cmd = ['7z', 'x', '-y', '-o' + opath, first_part]
        _, stderr, code = await cmd_exec(cmd)
        if code == 0:
            LOGGER.info("Split archive joined successfully via 7z")
            for f in files:
                if re_search(r'\.\d+$|\.part\d+\.rar$', f):
                    await remove(ospath.join(opath, f))
        else:
            LOGGER.error(f"7z join failed: {stderr}")
    elif first_part.endswith('.rar'):
        cmd = ['unrar', 'x', '-y', first_part, opath]
        _, stderr, code = await cmd_exec(cmd)
        if code != 0:
            LOGGER.error(f"unrar failed: {stderr}")


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
    if not await aiopath.isfile(file_path):
        return False
    mime_type = await sync_to_async(get_mime_type, file_path)
    if mime_type.startswith('video'):
        return True
    try:
        result = await cmd_exec(['ffprobe', '-v', 'error', '-show_entries', 'format=start_time,duration', '-of', 'csv=p=0', file_path])
        if result[0]:
            return True
    except:
        pass
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
        pattern = r"(\d+)\s+bytes|Total Physical Size\s*=\s*(\d+)"
        while not (
            self._listener.subproc.returncode is not None
            or self._listener.is_cancelled
            or self._listener.subproc.stdout.at_eof()
        ):
            try:
                line = await wait_for(self._listener.subproc.stdout.readline(), 2)
            except:
                break
            line = line.decode().strip()
            if match := re_search(pattern, line):
                self._listener.subsize = int(match[1] or match[2])
            await sleep(0.05)
        s = b""
        while not (
            self._listener.is_cancelled
            or self._listener.subproc.returncode is not None
            or self._listener.subproc.stdout.at_eof()
        ):
            try:
                char = await wait_for(self._listener.subproc.stdout.read(1), 60)
            except:
                break
            if not char:
                break
            s += char
            if char == b"%":
                try:
                    self._percentage = s.decode().rsplit(" ", 1)[-1].strip()
                    self._processed_bytes = (
                        int(self._percentage.strip("%")) / 100
                    ) * self._listener.subsize
                except:
                    self._processed_bytes = 0
                    self._percentage = "0%"
                s = b""
            await sleep(0.05)

        self._processed_bytes = 0
        self._percentage = "0%"

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
        if not pswd:
            del cmd[2]
        if self._listener.is_cancelled:
            return False
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
        if code == -9:
            self._listener.is_cancelled = True
            return False
        elif code != 0:
            try:
                stderr = stderr.decode().strip()
            except:
                stderr = "Unable to decode the error!"
            LOGGER.error(f"{stderr}. Unable to extract archive!. Path: {f_path}")
        return code

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
            if not pswd:
                del cmd[4]
            LOGGER.info(f"Zip: orig_path: {dl_path}, zip_path: {up_path}.0*")
        else:
            del cmd[1]
            if not pswd:
                del cmd[3]
            LOGGER.info(f"Zip: orig_path: {dl_path}, zip_path: {up_path}")
        if self._listener.is_cancelled:
            return False
        self._listener.subproc = await create_subprocess_exec(
            *cmd, stdout=PIPE, stderr=PIPE
        )
        await self._sevenz_progress()
        _, stderr = await self._listener.subproc.communicate()
        code = self._listener.subproc.returncode
        if self._listener.is_cancelled:
            return False
        if code == -9:
            self._listener.is_cancelled = True
            return False
        elif code == 0:
            await clean_target(dl_path)
            return up_path
        else:
            if await aiopath.exists(up_path):
                await remove(up_path)
            try:
                stderr = stderr.decode().strip()
            except:
                stderr = "Unable to decode the error!"
            LOGGER.error(f"{stderr}. Unable to zip this path: {dl_path}")
            return dl_path
