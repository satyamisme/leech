import os
import subprocess
import glob
import json
from bot import LOGGER

# Constants for splitting
MAX_TELEGRAM_SIZE = 2000 * 1024 * 1024
TARGET_SPLIT_SIZE_MB = 1990

# Constants for ffmpeg fallback
TARGET_MIN_SIZE_GB = 1.95 * 1024 * 1024 * 1024
MAX_ATTEMPTS_FFMPEG = 10
MIN_SPLIT_SECONDS_FFMPEG = 300
MAX_SPLIT_SECONDS_FFMPEG = 5400

def get_video_info(filepath):
    """Uses ffprobe to get video metadata."""
    command = ["ffprobe", "-v", "error", "-of", "json", "-show_entries", "format=duration,size", filepath]
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=60)
        return json.loads(result.stdout)
    except Exception as e:
        LOGGER.error(f"ffprobe failed: {e}")
        return None

def smart_split_if_needed(file_path: str) -> list:
    """
    Splits a video file using mkvmerge as the primary method.
    If mkvmerge fails, it falls back to an ffmpeg-based binary search.
    """
    try:
        file_size = os.path.getsize(file_path)
    except OSError as e:
        LOGGER.error(f"Could not get size of file {file_path}: {e}")
        return []

    if file_size <= MAX_TELEGRAM_SIZE:
        LOGGER.info(f"File ≤ 2000MiB. No split needed.")
        return [file_path]

    dir_name = os.path.dirname(file_path)
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    output_pattern = os.path.join(dir_name, f"{base_name} - Part %03d.mkv")

    LOGGER.info(f"✂️ Splitting {file_path} with mkvmerge...")

    mkvmerge_cmd = [
        "mkvmerge", "-o", output_pattern,
        "--split", f"size:{TARGET_SPLIT_SIZE_MB}M",
        file_path
    ]

    try:
        result = subprocess.run(mkvmerge_cmd, capture_output=True, text=True, timeout=600)
        if result.returncode == 0:
            split_files = sorted(glob.glob(os.path.join(dir_name, f"{base_name} - Part *.mkv")))
            if len(split_files) > 1:
                oversized = [f for f in split_files if os.path.getsize(f) >= MAX_TELEGRAM_SIZE]
                if not oversized:
                    LOGGER.info(f"✅ mkvmerge split successful! Created {len(split_files)} parts.")
                    return split_files
                else:
                    LOGGER.warning(f"❌ mkvmerge created oversized parts. Falling back to ffmpeg.")
            else:
                 LOGGER.warning("mkvmerge created only one part. Falling back to ffmpeg.")
        else:
            LOGGER.warning(f"mkvmerge failed. Stderr: {result.stderr[:200]}... Falling back to ffmpeg.")
    except Exception as e:
        LOGGER.error(f"An exception occurred while running mkvmerge: {e}. Falling back to ffmpeg.")

    for part in glob.glob(output_pattern.replace("%03d", "*")):
        try: os.remove(part)
        except OSError: pass

    return binary_search_split_fallback(file_path, dir_name, base_name, file_size)

def binary_search_split_fallback(file_path, dir_name, base_name, file_size):
    """Fallback splitting logic using ffmpeg and a binary search algorithm."""
    LOGGER.info("🔁 Falling back to ffmpeg binary search split...")
    output_pattern = os.path.join(dir_name, f"{base_name} - Part %03d.mkv")
    low = MIN_SPLIT_SECONDS_FFMPEG
    high = MAX_SPLIT_SECONDS_FFMPEG

    probe = get_video_info(file_path)
    if probe and probe.get("format"):
        try:
            duration = float(probe["format"]["duration"])
            bitrate_bps = int(probe["format"].get("bit_rate") or (file_size * 8) / max(duration, 1))
            target_bits = 1.98 * 1024 * 1024 * 1024 * 8
            ideal_sec = target_bits / bitrate_bps

            temp_low = max(MIN_SPLIT_SECONDS_FFMPEG, int(ideal_sec * 0.8))
            temp_high = min(MAX_SPLIT_SECONDS_FFMPEG, int(ideal_sec * 1.2))
            if temp_low <= temp_high:
                low, high = temp_low, temp_high
        except:
            pass

    best_split = None

    for attempt in range(1, MAX_ATTEMPTS_FFMPEG + 1):
        if low > high:
            break

        duration_sec = (low + high) // 2
        LOGGER.info(f"🔁 ffmpeg Attempt {attempt}/{MAX_ATTEMPTS_FFMPEG}: Trying duration {duration_sec}s.")

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
        small_early = [f for f in split_files[:-1] if os.path.getsize(f) < TARGET_MIN_SIZE_GB]

        if not oversized and not small_early:
            LOGGER.info(f"✅ ffmpeg fallback split successful with duration {duration_sec}s!")
            return split_files
        else:
            if oversized:
                high = duration_sec - 1
            elif small_early:
                low = duration_sec + 1
            if not oversized:
                best_split = split_files

    if best_split:
        LOGGER.warning("ffmpeg could not find a perfect split. Using best valid split found.")
        return best_split

    LOGGER.error("❌ ffmpeg fallback also failed. Returning original file path as last resort.")
    return [file_path]
