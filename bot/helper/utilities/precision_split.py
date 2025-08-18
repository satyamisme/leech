import os
import subprocess
import glob
import json
from bot import LOGGER

# Constants for splitting
MAX_TELEGRAM_SIZE = 2000 * 1024 * 1024  # 2,097,152,000 bytes
TARGET_MIN_SIZE = 1.95 * 1024 * 1024 * 1024  # 1.95 GB
TARGET_MAX_SIZE = 1.99 * 1024 * 1024 * 1024  # 1.99 GB
MAX_ATTEMPTS = 10
MIN_SPLIT_SECONDS = 300  # 5 minutes
MAX_SPLIT_SECONDS = 5400  # 90 minutes, a safe upper cap

def get_video_info(filepath):
    """Uses ffprobe to get video metadata."""
    command = [
        "ffprobe", "-v", "error", "-of", "json",
        "-show_entries", "format=duration,size,bit_rate",
        filepath
    ]
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=60)
        return json.loads(result.stdout)
    except Exception as e:
        LOGGER.error(f"ffprobe failed: {e}")
        return None

def is_valid_part_size(file_path, is_last_part=False):
    """Checks if a file part size is valid."""
    size = os.path.getsize(file_path)
    if is_last_part:
        return size < MAX_TELEGRAM_SIZE
    return TARGET_MIN_SIZE <= size < MAX_TELEGRAM_SIZE

def binary_search_split_if_needed(file_path: str) -> list:
    """
    Splits a video file using a binary search algorithm to find the optimal
    segment time that results in parts of the desired size.
    """
    try:
        file_size = os.path.getsize(file_path)
    except OSError as e:
        LOGGER.error(f"Could not get size of file {file_path}: {e}")
        return []

    if file_size <= MAX_TELEGRAM_SIZE:
        LOGGER.info(f"File size is within the Telegram limit. No split needed.")
        return [file_path]

    dir_name = os.path.dirname(file_path)
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    output_pattern = os.path.join(dir_name, f"{base_name} - Part %03d.mkv")

    LOGGER.info(f"✂️ Starting binary search split for: {file_path}")

    # --- Set up binary search range ---
    low = MIN_SPLIT_SECONDS
    high = MAX_SPLIT_SECONDS

    probe = get_video_info(file_path)
    if probe and probe.get("format"):
        try:
            duration = float(probe["format"]["duration"])
            bitrate_bps = int(probe["format"].get("bit_rate") or (file_size * 8) / max(duration, 1))

            # Calculate ideal duration for ~1.98 GB parts
            target_bits = TARGET_MAX_SIZE * 0.99 * 8
            ideal_duration_sec = target_bits / bitrate_bps

            # Set a tighter search range based on the ideal duration
            low = max(MIN_SPLIT_SECONDS, int(ideal_duration_sec * 0.8))
            high = min(MAX_SPLIT_SECONDS, int(ideal_duration_sec * 1.2))
            LOGGER.info(f"Bitrate-based ideal duration is ~{int(ideal_duration_sec/60)} min. Search range: [{low}s, {high}s]")
        except Exception as e:
            LOGGER.error(f"Bitrate estimation failed: {e}. Using default search range.")
    else:
        LOGGER.warning("ffprobe failed. Using default search range [{low}s, {high}s].")

    best_split_files = []

    for attempt in range(1, MAX_ATTEMPTS + 1):
        if low > high:
            LOGGER.info("Binary search range exhausted.")
            break

        duration_sec = (low + high) // 2
        LOGGER.info(f"🔁 Attempt {attempt}/{MAX_ATTEMPTS}: Trying duration {duration_sec}s (~{duration_sec//60} min). Range=[{low}, {high}]")

        # Clean old parts before each attempt
        for old in glob.glob(output_pattern.replace("%03d", "*")):
            try: os.remove(old)
            except OSError: pass

        command = [
            "ffmpeg", "-i", file_path, "-c", "copy", "-map", "0", "-f", "segment",
            "-segment_time", str(duration_sec), "-reset_timestamps", "1",
            "-avoid_negative_ts", "make_zero", "-y", output_pattern
        ]

        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=900)
            if result.returncode != 0:
                LOGGER.warning(f"ffmpeg failed. Assuming duration {duration_sec}s was too short.")
                low = duration_sec + 1
                continue

            split_files = sorted(glob.glob(output_pattern.replace("%03d", "*")))
            if not split_files or (len(split_files) == 1 and file_size > MAX_TELEGRAM_SIZE):
                LOGGER.warning("Only one part created. Duration is too long.")
                high = duration_sec - 1
                continue

            # --- Verify part sizes ---
            all_parts_valid = True
            for i, part_path in enumerate(split_files):
                is_last = (i == len(split_files) - 1)
                if not is_valid_part_size(part_path, is_last):
                    all_parts_valid = False
                    size_gb = os.path.getsize(part_path) / (1024**3)
                    if os.path.getsize(part_path) >= MAX_TELEGRAM_SIZE:
                        LOGGER.warning(f"❌ Part {i+1} is oversized ({size_gb:.2f} GB). Duration is too long.")
                        high = duration_sec - 1
                    else:
                        LOGGER.warning(f"❌ Part {i+1} is too small ({size_gb:.2f} GB). Duration is too short.")
                        low = duration_sec + 1
                    break # Stop checking parts on the first failure

            if all_parts_valid:
                LOGGER.info(f"✅ Perfect split found at {duration_sec}s! {len(split_files)} parts created.")
                return split_files
            else:
                # If not perfect but not oversized, keep it as a potential best option
                if not any(os.path.getsize(f) >= MAX_TELEGRAM_SIZE for f in split_files):
                    best_split_files = split_files

        except Exception as e:
            LOGGER.error(f"An exception occurred during split attempt {attempt}: {e}")
            high = duration_sec - 1 # Assume duration was too long on generic error
            continue

    if best_split_files:
        LOGGER.warning("Could not find a perfect split. Using the best valid split found.")
        return best_split_files

    # --- Ultimate Fallback ---
    LOGGER.error("❌ All attempts failed. Falling back to a safe 60-minute split.")
    for old in glob.glob(output_pattern.replace("%03d", "*")):
        try: os.remove(old)
        except OSError: pass

    fallback_command = [
        "ffmpeg", "-i", file_path, "-c", "copy", "-map", "0",
        "-f", "segment", "-segment_time", "3600", "-y", output_pattern
    ]
    subprocess.run(fallback_command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    final_parts = sorted(glob.glob(output_pattern.replace("%03d", "*")))
    return final_parts if final_parts else []
