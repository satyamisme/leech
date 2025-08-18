import os
import subprocess
import glob
import json
from bot import LOGGER

# Telegram's maximum file size is 2000 MiB
MAX_TELEGRAM_SIZE = 2000 * 1024 * 1024  # 2,097,152,000 bytes

# Target size range for each part (except the last one)
TARGET_MIN_SIZE = 1.95 * 1024 * 1024 * 1024  # 1.95 GB
TARGET_MAX_SIZE = 1.99 * 1024 * 1024 * 1024  # 1.99 GB

# Retry and duration settings
MAX_ATTEMPTS = 10
MIN_SPLIT_SECONDS = 300  # 5 minutes, a safe minimum for split duration

def get_video_info(filepath):
    """
    Uses ffprobe to get video metadata.
    Returns a dictionary with format info or None on failure.
    """
    command = [
        "ffprobe", "-v", "error", "-of", "json",
        "-show_entries", "format=duration,size,bit_rate",
        filepath
    ]
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=60
        )
        if result.returncode == 0:
            return json.loads(result.stdout)
        else:
            LOGGER.error(f"ffprobe failed with return code {result.returncode}: {result.stderr}")
            return None
    except Exception as e:
        LOGGER.error(f"An exception occurred while running ffprobe: {e}")
        return None

def precision_split_if_needed(file_path: str) -> list:
    """
    Splits a video file if it's larger than MAX_TELEGRAM_SIZE.
    It adaptively retries with different durations to ensure parts are
    within the target size range.
    """
    try:
        file_size = os.path.getsize(file_path)
    except OSError as e:
        LOGGER.error(f"Could not get size of file {file_path}: {e}")
        return []

    if file_size <= MAX_TELEGRAM_SIZE:
        LOGGER.info(f"File size ({file_size / (1024**3):.2f} GB) is within the Telegram limit. No split needed.")
        return [file_path]

    dir_name = os.path.dirname(file_path)
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    LOGGER.info(f"✂️ File is too large. Starting precision split for: {file_path}")

    # --- Initial Duration Calculation ---
    duration_sec = 3600  # Default to 60 minutes
    probe_data = get_video_info(file_path)

    if probe_data and probe_data.get("format"):
        try:
            format_info = probe_data["format"]
            duration = float(format_info["duration"])
            file_bytes = int(format_info["size"])
            bitrate_bps = int(format_info.get("bit_rate") or (file_bytes * 8) / max(duration, 1))

            # Predict the ideal duration to get parts of ~1.98 GB
            target_bits = TARGET_MAX_SIZE * 0.99 * 8
            ideal_duration_sec = max(MIN_SPLIT_SECONDS, target_bits / bitrate_bps)
            duration_sec = int(ideal_duration_sec)
            LOGGER.info(f"Bitrate-based ideal split duration is ~{duration_sec // 60} minutes.")
        except Exception as e:
            LOGGER.error(f"Failed to calculate ideal duration from ffprobe data: {e}. Starting with default 60 min.")
    else:
        LOGGER.warning("ffprobe failed. Starting with default 60 min split duration.")

    # --- Retry Loop ---
    output_pattern = os.path.join(dir_name, f"{base_name} - Part %03d.mkv")
    for attempt in range(1, MAX_ATTEMPTS + 1):
        # Clean up parts from any previous failed attempt
        for old_part in glob.glob(output_pattern.replace("%03d", "*")):
            try:
                os.remove(old_part)
            except OSError:
                pass

        LOGGER.info(f"🔁 Attempt {attempt}/{MAX_ATTEMPTS}: Splitting with segment time of {duration_sec}s (~{duration_sec//60} min).")

        command = [
            "ffmpeg", "-i", file_path,
            "-c", "copy", "-map", "0",
            "-f", "segment",
            "-segment_time", str(duration_sec),
            "-reset_timestamps", "1",
            "-avoid_negative_ts", "make_zero",
            "-y", output_pattern
        ]

        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=900)
            if result.returncode != 0:
                LOGGER.warning(f"ffmpeg process failed on attempt {attempt}. Stderr: {result.stderr[:200]}...")
                duration_sec = int(duration_sec * 0.90) # Reduce duration significantly on ffmpeg error
                continue

            split_files = sorted(glob.glob(os.path.join(dir_name, f"{base_name} - Part *.mkv")))
            if not split_files or (len(split_files) == 1 and file_size > MAX_TELEGRAM_SIZE):
                LOGGER.warning(f"Attempt {attempt} resulted in one or zero parts. The duration is likely too long.")
                duration_sec = int(duration_sec * 0.85) # Decrease duration aggressively
                continue

            # --- Verification of Parts ---
            oversized_parts = [f for f in split_files if os.path.getsize(f) >= MAX_TELEGRAM_SIZE]
            small_early_parts = [f for f in split_files[:-1] if os.path.getsize(f) < TARGET_MIN_SIZE]

            if not oversized_parts and not small_early_parts:
                LOGGER.info(f"✅ Perfect split achieved on attempt {attempt} with {len(split_files)} parts!")
                for f in split_files:
                    gb_size = os.path.getsize(f) / (1024**3)
                    LOGGER.info(f"   -> {os.path.basename(f)} ({gb_size:.2f} GB)")
                return split_files
            else:
                # --- Adaptive Duration Adjustment ---
                if oversized_parts:
                    LOGGER.warning(f"❌ Attempt {attempt} failed: {len(oversized_parts)} part(s) were >= 2000 MiB. Reducing duration.")
                    duration_sec = int(duration_sec * 0.95) # Decrease duration by 5%
                if small_early_parts:
                    LOGGER.warning(f"❌ Attempt {attempt} failed: {len(small_early_parts)} early part(s) were < 1.95 GB. Increasing duration.")
                    duration_sec = int(duration_sec * 1.05) # Increase duration by 5%

        except subprocess.TimeoutExpired:
            LOGGER.error(f"ffmpeg command timed out on attempt {attempt}. Retrying with shorter duration.")
            duration_sec = int(duration_sec * 0.90)
            continue
        except Exception as e:
            LOGGER.error(f"An exception occurred during split attempt {attempt}: {e}")
            duration_sec = int(duration_sec * 0.90)
            continue

    # --- Ultimate Fallback ---
    LOGGER.error("❌ All adaptive split attempts failed. Falling back to a safe 60-minute split.")
    for old_part in glob.glob(output_pattern.replace("%03d", "*")):
        try:
            os.remove(old_part)
        except OSError:
            pass

    fallback_command = [
        "ffmpeg", "-i", file_path, "-c", "copy", "-map", "0",
        "-f", "segment", "-segment_time", "3600", "-y", output_pattern
    ]
    subprocess.run(fallback_command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    final_parts = sorted(glob.glob(os.path.join(dir_name, f"{base_name} - Part *.mkv")))
    if not final_parts:
        LOGGER.error("Fallback split also failed to produce any files.")
        return []

    return final_parts
