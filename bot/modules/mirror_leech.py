from aiofiles.os import path as aiopath
from base64 import b64encode
from re import match as re_match

from .. import LOGGER, bot_loop, task_dict_lock, DOWNLOAD_DIR
from ..helper.ext_utils.bot_utils import (
    get_content_type,
    sync_to_async,
    arg_parser,
    COMMAND_USAGE,
)
from ..helper.ext_utils.exceptions import DirectDownloadLinkException
from ..helper.ext_utils.links_utils import (
    is_url,
    is_magnet,
    is_gdrive_link,
    is_rclone_path,
    is_telegram_link,
    is_gdrive_id,
)
from ..helper.listeners.task_listener import TaskListener
from ..helper.mirror_leech_utils.download_utils.aria2_download import (
    add_aria2_download,
)
from ..helper.mirror_leech_utils.download_utils.direct_downloader import (
    add_direct_download,
)
from ..helper.mirror_leech_utils.download_utils.direct_link_generator import (
    direct_link_generator,
)
from ..helper.mirror_leech_utils.download_utils.gd_download import add_gd_download
from ..helper.mirror_leech_utils.download_utils.jd_download import add_jd_download
from ..helper.mirror_leech_utils.download_utils.qbit_download import add_qb_torrent
from ..helper.mirror_leech_utils.download_utils.nzb_downloader import add_nzb
from ..helper.mirror_leech_utils.download_utils.rclone_download import (
    add_rclone_download,
)
from ..helper.mirror_leech_utils.download_utils.telegram_download import (
    TelegramDownloadHelper,
)
from ..helper.telegram_helper.message_utils import send_message, get_tg_link_message


class Mirror(TaskListener):
    def __init__(
        self,
        client,
        message,
        is_qbit=False,
        is_leech=False,
        is_jd=False,
        is_nzb=False,
        same_dir=None,
        bulk=None,
        multi_tag=None,
        options="",
    ):
        if same_dir is None:
            same_dir = {}
        if bulk is None:
            bulk = []
        self.message = message
        self.client = client
        self.multi_tag = multi_tag
        self.options = options
        self.same_dir = same_dir
        self.bulk = bulk
        super().__init__()
        self.is_qbit = is_qbit
        self.is_leech = is_leech
        self.is_jd = is_jd
        self.is_nzb = is_nzb

    async def new_event(self):
        text = self.message.text.split("\n")
        input_list = text[0].split(" ")

        self.args = {
            "-doc": False,
            "-med": False,
            "-d": False,
            "-j": False,
            "-s": False,
            "-b": False,
            "-e": False,
            "-z": False,
            "-sv": False,
            "-ss": False,
            "-f": False,
            "-fd": False,
            "-fu": False,
            "-hl": False,
            "-bt": False,
            "-ut": False,
            "-i": 0,
            "-sp": 0,
            "link": "",
            "-n": "",
            "-m": "",
            "-up": "",
            "-rcf": "",
            "-au": "",
            "-ap": "",
            "-h": [],
            "-t": "",
            "-ca": "",
            "-cv": "",
            "-ns": "",
            "-tl": "",
            "-ff": set(),
        }

        arg_parser(input_list[1:], self.args)

        self.select = self.args["-s"]
        self.seed = self.args["-d"]
        self.name = self.args["-n"]
        self.up_dest = self.args["-up"]
        self.rc_flags = self.args["-rcf"]
        self.link = self.args["link"]
        self.compress = self.args["-z"]
        self.extract = self.args["-e"]
        self.join = self.args["-j"]
        self.thumb = self.args["-t"]
        self.split_size = self.args["-sp"]
        self.sample_video = self.args["-sv"]
        self.screen_shots = self.args["-ss"]
        self.force_run = self.args["-f"]
        self.force_download = self.args["-fd"]
        self.force_upload = self.args["-fu"]
        self.convert_audio = self.args["-ca"]
        self.convert_video = self.args["-cv"]
        self.name_sub = self.args["-ns"]
        self.hybrid_leech = self.args["-hl"]
        self.thumbnail_layout = self.args["-tl"]
        self.as_doc = self.args["-doc"]
        self.as_med = self.args["-med"]
        self.folder_name = f"/{self.args['-m']}".rstrip("/") if len(self.args["-m"]) > 0 else ""
        self.bot_trans = self.args["-bt"]
        self.user_trans = self.args["-ut"]
        self.ffmpeg_cmds = self.args["-ff"]

        self.headers = self.args["-h"]
        if self.headers:
            self.headers = self.headers.split("|")
        is_bulk = self.args["-b"]

        bulk_start = 0
        bulk_end = 0
        self.ratio = None
        self.seed_time = None
        self.reply_to = None
        self.file_ = None
        self.session = ""

        try:
            self.multi = int(self.args["-i"])
        except:
            self.multi = 0

        if not isinstance(self.seed, bool):
            dargs = self.seed.split(":")
            self.ratio = dargs[0] or None
            if len(dargs) == 2:
                self.seed_time = dargs[1] or None
            self.seed = True

        if not isinstance(is_bulk, bool):
            dargs = is_bulk.split(":")
            bulk_start = dargs[0] or 0
            if len(dargs) == 2:
                bulk_end = dargs[1] or 0
            is_bulk = True

        if not is_bulk:
            if self.multi > 0:
                if self.folder_name:
                    async with task_dict_lock:
                        if self.folder_name in self.same_dir:
                            self.same_dir[self.folder_name]["tasks"].add(self.mid)
                            for fd_name in self.same_dir:
                                if fd_name != self.folder_name:
                                    self.same_dir[fd_name]["total"] -= 1
                        elif self.same_dir:
                            self.same_dir[self.folder_name] = {
                                "total": self.multi,
                                "tasks": {self.mid},
                            }
                            for fd_name in self.same_dir:
                                if fd_name != self.folder_name:
                                    self.same_dir[fd_name]["total"] -= 1
                        else:
                            self.same_dir = {
                                self.folder_name: {
                                    "total": self.multi,
                                    "tasks": {self.mid},
                                }
                            }
                elif self.same_dir:
                    async with task_dict_lock:
                        for fd_name in self.same_dir:
                            self.same_dir[fd_name]["total"] -= 1
        else:
            await self.init_bulk(input_list, bulk_start, bulk_end, Mirror)
            return

        if len(self.bulk) != 0:
            del self.bulk[0]

        await self.run_multi(input_list, Mirror)

        await self.get_tag(text)

        self.path = f"{DOWNLOAD_DIR}{self.mid}{self.folder_name}"

        if not self.link:
            reply_to = self.message.reply_to_message
            if reply_to is not None:
                self.reply_to = reply_to
                if reply_to.text:
                    self.link = reply_to.text.split("\n", 1)[0].strip()

        if is_telegram_link(self.link):
            try:
                self.reply_to, self.session = await get_tg_link_message(self.link)
            except Exception as e:
                await send_message(self.message, f"ERROR: {e}")
                await self.remove_from_same_dir()
                return

        if isinstance(self.reply_to, list):
            self.bulk = self.reply_to
            b_msg = input_list[:1]
            self.options = " ".join(input_list[1:])
            b_msg.append(f"{self.bulk[0]} -i {len(self.bulk)} {self.options}")
            nextmsg = await send_message(self.message, " ".join(b_msg))
            nextmsg = await self.client.get_messages(
                chat_id=self.message.chat.id, message_ids=nextmsg.id
            )
            if self.message.from_user:
                nextmsg.from_user = self.user
            else:
                nextmsg.sender_chat = self.user
            await Mirror(
                self.client,
                nextmsg,
                self.is_qbit,
                self.is_leech,
                self.is_jd,
                self.is_nzb,
                self.same_dir,
                self.bulk,
                self.multi_tag,
                self.options,
            ).new_event()
            return

        if self.reply_to:
            self.file_ = (
                self.reply_to.document
                or self.reply_to.photo
                or self.reply_to.video
                or self.reply_to.audio
                or self.reply_to.voice
                or self.reply_to.video_note
                or self.reply_to.sticker
                or self.reply_to.animation
                or None
            )

            if self.file_ is None:
                if self.reply_to.text:
                    self.link = self.reply_to.text.split("\n", 1)[0].strip()
                else:
                    self.reply_to = None
            elif self.reply_to.document and (
                self.file_.mime_type == "application/x-bittorrent"
                or self.file_.file_name.endswith((".torrent", ".dlc", ".nzb"))
            ):
                self.link = await self.reply_to.download()
                self.file_ = None

        if (
            not self.link
            and self.file_ is None
            or is_telegram_link(self.link)
            and self.reply_to is None
            or self.file_ is None
            and not is_url(self.link)
            and not is_magnet(self.link)
            and not await aiopath.exists(self.link)
            and not is_rclone_path(self.link)
            and not is_gdrive_id(self.link)
            and not is_gdrive_link(self.link)
        ):
            await send_message(
                self.message, COMMAND_USAGE["mirror"][0], COMMAND_USAGE["mirror"][1]
            )
            await self.remove_from_same_dir()
            return

        if len(self.link) > 0:
            LOGGER.info(self.link)

        try:
            await self.before_start()
        except Exception as e:
            await send_message(self.message, e)
            await self.remove_from_same_dir()
            return

        await self.on_task_created()


async def mirror(client, message):
    bot_loop.create_task(Mirror(client, message).new_event())


async def qb_mirror(client, message):
    bot_loop.create_task(Mirror(client, message, is_qbit=True).new_event())


async def jd_mirror(client, message):
    bot_loop.create_task(Mirror(client, message, is_jd=True).new_event())


async def nzb_mirror(client, message):
    bot_loop.create_task(Mirror(client, message, is_nzb=True).new_event())


async def leech(client, message):
    bot_loop.create_task(Mirror(client, message, is_leech=True).new_event())


async def qb_leech(client, message):
    bot_loop.create_task(
        Mirror(client, message, is_qbit=True, is_leech=True).new_event()
    )


async def jd_leech(client, message):
    bot_loop.create_task(Mirror(client, message, is_leech=True, is_jd=True).new_event())


async def nzb_leech(client, message):
    bot_loop.create_task(
        Mirror(client, message, is_leech=True, is_nzb=True).new_event()
    )