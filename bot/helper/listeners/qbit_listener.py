from aiofiles.os import remove, path as aiopath
from asyncio import sleep, TimeoutError
from time import time
from aiohttp.client_exceptions import ClientError
from aioqbt.exc import AQError

from ... import (
    task_dict,
    task_dict_lock,
    intervals,
    qb_torrents,
    qb_listener_lock,
    LOGGER,
)
from ...core.config_manager import Config
# from ...core.config_manager import (
#     Config,
#     qbit_options,
#     set_qbit_options,
#     save_config,
# )
from ...core.torrent_manager import TorrentManager
from ..ext_utils.bot_utils import new_task
from ..ext_utils.files_utils import clean_unwanted
from ..ext_utils.status_utils import get_readable_time, get_task_by_gid
from ..ext_utils.task_manager import stop_duplicate_check
from ..mirror_leech_utils.status_utils.qbit_status import QbittorrentStatus
from ..telegram_helper.message_utils import update_status_message

CLIENT = TorrentManager().qbittorrent


async def _remove_torrent(hash_, tag):
    try:
        await CLIENT.torrents_delete(torrent_hashes=hash_, delete_files=True)
    except Exception as e:
        LOGGER.error(f"Failed to delete torrent: {e}")
    finally:
        async with qb_listener_lock:
            if tag in qb_torrents:
                del qb_torrents[tag]
        await CLIENT.torrents_delete_tags(tags=tag)


@new_task
async def _on_download_error(err, tor, button=None):
    LOGGER.info(f"Cancelling Download: {tor.name}")
    ext_hash = tor.hash
    if task := await get_task_by_gid(ext_hash[:12]):
        await task.listener.on_download_error(err, button)
    await CLIENT.torrents_pause(torrent_hashes=ext_hash)
    await sleep(0.3)
    await _remove_torrent(ext_hash, tor.tags[0])


@new_task
async def _on_seed_finish(tor):
    ext_hash = tor.hash
    LOGGER.info(f"Cancelling Seed: {tor.name}")
    if task := await get_task_by_gid(ext_hash[:12]):
        msg = f"Seeding stopped with Ratio: {round(tor.ratio, 3)} and Time: {get_readable_time(int(tor.seeding_time.total_seconds() or "0"))}"
        await task.listener.on_upload_error(msg)
    await _remove_torrent(ext_hash, tor.tags[0])


@new_task
async def _stop_duplicate(tor):
    if task := await get_task_by_gid(tor.hash[:12]):
        if task.listener.stop_duplicate:
            task.listener.name = tor.content_path.rsplit("/", 1)[-1].rsplit(".!qB", 1)[
                0
            ]
            msg, button = await stop_duplicate_check(task.listener)
            if msg:
                _on_download_error(msg, tor, button)


@new_task
async def _on_download_complete(tor):
    ext_hash = tor.hash
    tag = tor.tags[0]
    if task := await get_task_by_gid(ext_hash[:12]):
        if not task.listener.seed:
            await CLIENT.torrents_pause(torrent_hashes=ext_hash)

        task.listener.name = tor.content_path.rsplit("/", 1)[-1].rsplit(".!qB", 1)[0]

        if task.listener.select:
            await clean_unwanted(task.listener.dir)
            try:
                files = await CLIENT.torrents_files(torrent_hash=ext_hash)
            except Exception as e:
                LOGGER.error(f"Failed to get torrent files: {e}")
            for f in files:
                if f.priority == 0 and await aiopath.exists(f"{path}/{f.name}"):
                    try:
                        await remove(f"{path}/{f.name}")
                    except:
                        pass
        await task.listener.on_download_complete()
        if intervals["stopAll"]:
            return
        if task.listener.seed and not task.listener.is_cancelled:
            async with task_dict_lock:
                if task.listener.mid in task_dict:
                    removed = False
                    task_dict[task.listener.mid] = QbittorrentStatus(
                        task.listener, True
                    )
                else:
                    removed = True
            if removed:
                await _remove_torrent(ext_hash, tag)
                return
            async with qb_listener_lock:
                if tag in qb_torrents:
                    qb_torrents[tag]["seeding"] = True
                else:
                    return
            await update_status_message(task.listener.message.chat.id)
            LOGGER.info(f"Seeding started: {tor.name} - Hash: {ext_hash}")
        else:
            await _remove_torrent(ext_hash, tag)
    else:
        await _remove_torrent(ext_hash, tag)


# async def _check_and_set_qbit_options():
#     try:
#         qbit_version = (await CLIENT.app_version()).split("v")[-1]
#         if qbit_version >= "4.5.4":
#             if qbit_options.get("max_checking_torrents") != -1:
#                 qbit_options["max_checking_torrents"] = -1
#                 await set_qbit_options(qbit_options)
#                 save_config()
#                 LOGGER.info(
#                     "qBittorrent version is >= 4.5.4, max_checking_torrents has been set to -1."
#                 )
#         elif qbit_options.get("max_checking_torrents") != 1:
#             qbit_options["max_checking_torrents"] = 1
#             await set_qbit_options(qbit_options)
#             save_config()
#             LOGGER.info(
#                 "qBittorrent version is < 4.5.4, max_checking_torrents has been set to 1."
#             )
#     except Exception as e:
#         LOGGER.error(f"Failed to check/set qBittorrent options: {e}")


@new_task
async def _qb_listener():
    # await _check_and_set_qbit_options()
    while True:
        async with qb_listener_lock:
            try:
                torrents = await CLIENT.torrents_info(
                    status_filter="completed", tag=Config.TORRENT_TAG
                )
                if len(torrents) == 0:
                    intervals["qb"] = ""
                    break
                for tor_info in torrents:
                    tag = tor_info.tags[0]
                    if tag not in qb_torrents:
                        continue
                    state = tor_info.state
                    if state == "metaDL":
                        qb_torrents[tag]["stalled_time"] = time()
                        if (
                            Config.TORRENT_TIMEOUT
                            and time() - qb_torrents[tag]["start_time"]
                            >= Config.TORRENT_TIMEOUT
                        ):
                            await _on_download_error("Torrent timed out!", tor_info)
                        else:
                            await CLIENT.torrents_reannounce(torrent_hashes=tor_info.hash)
                    elif state == "downloading":
                        qb_torrents[tag]["stalled_time"] = time()
                        if not qb_torrents[tag]["stop_dup_check"]:
                            qb_torrents[tag]["stop_dup_check"] = True
                            await _stop_duplicate(tor_info)
                    elif state == "stalledDL":
                        if (
                            not qb_torrents[tag]["rechecked"]
                            and 0.99989999999999999 < tor_info.progress < 1
                        ):
                            msg = f"Force recheck - Name: {tor_info.name} Hash: "
                            msg += f"{tor_info.hash} Downloaded Bytes: {tor_info.downloaded} "
                            msg += f"Size: {tor_info.size} Total Size: {tor_info.total_size}"
                            LOGGER.info(msg)
                            await CLIENT.torrents_recheck(torrent_hashes=tor_info.hash)
                            qb_torrents[tag]["rechecked"] = True
                        elif (
                            Config.TORRENT_TIMEOUT
                            and time() - qb_torrents[tag]["stalled_time"]
                            >= Config.TORRENT_TIMEOUT
                        ):
                            await _on_download_error("Torrent stalled!", tor_info)
                        else:
                            await CLIENT.torrents_reannounce(torrent_hashes=tor_info.hash)
                    elif state == "missingFiles":
                        await CLIENT.torrents_recheck(torrent_hashes=tor_info.hash)
                    elif state == "error":
                        await _on_download_error(
                            f"qB Error: {tor_info.state_message}", tor_info
                        )
                    elif (
                        int(tor_info.completion_on.timestamp()) != -1
                        and not qb_torrents[tag]["uploaded"]
                        and state
                        in [
                            "queuedUP",
                            "stalledUP",
                            "uploading",
                            "forcedUP",
                        ]
                    ):
                        qb_torrents[tag]["uploaded"] = True
                        await _on_download_complete(tor_info)
                    elif (
                        state in ["stoppedUP", "stoppedDL"]
                        and qb_torrents[tag]["seeding"]
                    ):
                        qb_torrents[tag]["seeding"] = False
                        await _on_seed_finish(tor_info)
                        await sleep(0.5)
            except (ClientError, TimeoutError, Exception, AQError) as e:
                LOGGER.error(str(e))
        await sleep(3)
    async with qb_listener_lock:
        intervals["qb"] = ""


async def on_download_start(tag):
    async with qb_listener_lock:
        qb_torrents[tag] = {
            "start_time": time(),
            "stalled_time": time(),
            "stop_dup_check": False,
            "rechecked": False,
            "uploaded": False,
            "seeding": False,
        }
        if not intervals["qb"]:
            intervals["qb"] = await _qb_listener()
