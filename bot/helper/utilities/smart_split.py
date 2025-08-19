import os
import subprocess
import glob
from bot import LOGGER

MAX_TELEGRAM_SIZE = 2000 * 1024 * 1024
TARGET_MIN_SIZE = 1950 * 1024 * 1024
TARGET_MAX_SIZE = 1999 * 1024 * 1024
MAX_ATTEMPTS = 10

def smart_split_if_needed(file_path: str) -> list:
    file_size = os.path.getsize(file_path)
    if file_size <= MAX_TELEGRAM_SIZE:
        return [file_path]

    dir_name = os.path.dirname(file_path)
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    output_pattern = os.path.join(dir_name, f"{base_name} - Part %03d.mkv")

    low = 1940
    high = 2000
    best_split = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        size_mb = (low + high) // 2
        if low > high:
            break

        LOGGER.info(f"🔁 Attempt {attempt}/{MAX_ATTEMPTS}: Trying mkvmerge --split size:{size_mb}M")

        for old in glob.glob(output_pattern.replace("%03d", "*")):
            try: os.remove(old)
            except: pass

        cmd = ["mkvmerge", "-o", output_pattern, "--split", f"size:{size_mb}M", file_path]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
            if result.returncode != 0:
                LOGGER.warning(f"mkvmerge failed. Assuming size {size_mb}M is too large. Reducing upper bound.")
                high = size_mb - 1
                continue

            split_files = sorted(glob.glob(os.path.join(dir_name, f"{base_name} - Part *.mkv")))
            if len(split_files) < 2:
                LOGGER.warning(f"Only one part created. Size {size_mb}M is too large. Reducing upper bound.")
                high = size_mb - 1
                continue

            first_part_size = os.path.getsize(split_files[0])
            if TARGET_MIN_SIZE <= first_part_size <= TARGET_MAX_SIZE:
                LOGGER.info(f"✅ Perfect split found with size {size_mb}M! Created {len(split_files)} parts.")
                return split_files

            if first_part_size > TARGET_MAX_SIZE:
                high = size_mb - 1
            else:
                low = size_mb + 1

            if all(os.path.getsize(f) < MAX_TELEGRAM_SIZE for f in split_files):
                best_split = split_files

        except Exception as e:
            LOGGER.error(f"mkvmerge execution failed: {e}")
            continue

    if best_split:
        LOGGER.warning("Could not achieve perfect size. Using best safe split found.")
        return best_split

    LOGGER.error("❌ All mkvmerge attempts failed. No safe split found.")
    return [file_path]
