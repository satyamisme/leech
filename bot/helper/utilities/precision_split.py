import os
import subprocess
import glob
from bot import LOGGER

MAX_TELEGRAM_SIZE = 2000 * 1024 * 1024  # 2000 MiB
TARGET_MIN_SIZE = 1990 * 1024 * 1024  # 1990 MiB
TARGET_MAX_SIZE = 1999 * 1024 * 1024  # 1999 MiB
MAX_ATTEMPTS = 10

def precision_split_if_needed(file_path: str) -> list:
    file_size = os.path.getsize(file_path)
    if file_size <= MAX_TELEGRAM_SIZE:
        return [file_path]

    dir_name = os.path.dirname(file_path)
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    output_pattern = os.path.join(dir_name, f"{base_name} - Part %03d.mkv")

    low = 1940  # MiB
    high = 2000  # MiB
    best_split = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        size_mb = (low + high) // 2
        LOGGER.info(f"🔁 Attempt {attempt}/{MAX_ATTEMPTS}: Trying mkvmerge --split size:{size_mb}M")

        for old in glob.glob(output_pattern.replace("%03d", "*")):
            try:
                os.remove(old)
            except:
                pass

        cmd = [
            "mkvmerge", "-o", output_pattern,
            "--split", f"size:{size_mb}M",
            file_path
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
            if result.returncode != 0:
                LOGGER.warning(f"mkvmerge failed: {result.stderr[:100]}")
                high = size_mb - 1
                continue

            split_files = sorted(glob.glob(os.path.join(dir_name, f"{base_name} - Part *.mkv")))
            if len(split_files) < 2:
                high = size_mb - 1
                continue

            early_parts = split_files[:-1]
            valid_parts = [
                f for f in early_parts
                if TARGET_MIN_SIZE <= os.path.getsize(f) <= TARGET_MAX_SIZE
            ]

            if len(valid_parts) == len(early_parts):
                LOGGER.info(f"✅ Perfect split! {len(split_files)} parts")
                return split_files
            else:
                first_size = os.path.getsize(split_files[0])
                if first_size > TARGET_MAX_SIZE:
                    high = size_mb - 1
                elif first_size < TARGET_MIN_SIZE:
                    low = size_mb + 1
                else:
                    low = size_mb + 1

                if all(os.path.getsize(f) < MAX_TELEGRAM_SIZE for f in split_files):
                    best_split = split_files

        except Exception as e:
            LOGGER.error(f"mkvmerge failed: {e}")
            continue

    return best_split or split_files
