import os
import subprocess
import glob
import json
from bot import LOGGER

# Constants
MAX_TELEGRAM_SIZE = 2000 * 1024 * 1024
TARGET_MIN_SIZE = 1.95 * 1024 * 1024 * 1024
MAX_ATTEMPTS = 10
SIZE_STEP_MB = 100

def get_video_info(filepath):
    """Uses ffprobe to get video metadata."""
    command = ["ffprobe", "-v", "error", "-of", "json", "-show_entries", "format=duration,size,bit_rate", filepath]
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=60)
        return json.loads(result.stdout)
    except Exception as e:
        LOGGER.error(f"ffprobe failed: {e}")
        return None

def smart_split_if_needed(file_path: str) -> list:
    """
    Splits a file using an iterative mkvmerge approach to find the perfect size.
    Falls back to ffmpeg if all mkvmerge attempts fail.
    """
    file_size = os.path.getsize(file_path)
    if file_size <= MAX_TELEGRAM_SIZE:
        LOGGER.info("File size is within the Telegram limit. No split needed.")
        return [file_path]

    dir_name = os.path.dirname(file_path)
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    output_pattern = os.path.join(dir_name, f"{base_name} - Part %03d.mkv")

    # --- mkvmerge Retry Logic ---
    sizes_to_try = [1990 - (i * 10) for i in range(MAX_ATTEMPTS)]

    for attempt, size_mb in enumerate(sizes_to_try, 1):
        LOGGER.info(f"🔁 Attempt {attempt}/{MAX_ATTEMPTS}: Trying mkvmerge --split size:{size_mb}M")

        for old in glob.glob(output_pattern.replace("%03d", "*")):
            try: os.remove(old)
            except: pass

        cmd = ["mkvmerge", "-o", output_pattern, "--split", f"size:{size_mb}M", file_path]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
            if result.returncode != 0:
                LOGGER.warning(f"mkvmerge failed on attempt {attempt}: {result.stderr[:100]}...")
                continue

            split_files = sorted(glob.glob(os.path.join(dir_name, f"{base_name} - Part *.mkv")))
            if len(split_files) < 2:
                LOGGER.warning(f"mkvmerge created only one part on attempt {attempt}.")
                continue

            # Validate all parts except the last one
            is_perfect = all(
                TARGET_MIN_SIZE <= os.path.getsize(f) < MAX_TELEGRAM_SIZE
                for f in split_files[:-1]
            )

            # Also ensure the last part is not oversized
            if is_perfect and os.path.getsize(split_files[-1]) < MAX_TELEGRAM_SIZE:
                LOGGER.info(f"✅ Perfect split found with size {size_mb}M! Created {len(split_files)} parts.")
                return split_files
            else:
                LOGGER.warning(f"❌ Attempt {attempt} with size {size_mb}M resulted in parts of the wrong size.")
                continue

        except Exception as e:
            LOGGER.error(f"mkvmerge execution failed on attempt {attempt}: {e}")
            continue

    LOGGER.error("❌ All mkvmerge attempts failed. Falling back to ffmpeg...")
    return binary_search_split_fallback(file_path, dir_name, base_name, file_size)

def binary_search_split_fallback(file_path, dir_name, base_name, file_size):
    """Fallback splitting logic using ffmpeg and a binary search algorithm."""
    LOGGER.info("🔁 Falling back to ffmpeg binary search split...")
    output_pattern = os.path.join(dir_name, f"{base_name} - Part %03d.mkv")
    low = 300
    high = 5400
    best_split = []

    for attempt in range(1, 11):
        if low > high:
            break
        duration_sec = (low + high) // 2
        for old in glob.glob(output_pattern.replace("%03d", "*")):
            try: os.remove(old)
            except: pass

        cmd = [
            "ffmpeg", "-i", file_path, "-c", "copy", "-map", "0",
            "-f", "segment", "-segment_time", str(duration_sec),
            "-reset_timestamps", "1", "-avoid_negative_ts", "make_zero", "-y", output_pattern
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=900)

        if result.returncode != 0:
            high = duration_sec - 1
            continue

        split_files = sorted(glob.glob(output_pattern.replace("%03d", "*")))
        if not split_files or (len(split_files) == 1 and file_size > MAX_TELEGRAM_SIZE):
            high = duration_sec - 1
            continue

        oversized = [f for f in split_files if os.path.getsize(f) >= MAX_TELEGRAM_SIZE]
        small_early = [f for f in split_files[:-1] if os.path.getsize(f) < TARGET_MIN_SIZE]

        if not oversized and not small_early:
            return split_files
        else:
            if oversized: high = duration_sec - 1
            else: low = duration_sec + 1
            if not oversized: best_split = split_files

    if best_split:
        return best_split

    return [file_path]
