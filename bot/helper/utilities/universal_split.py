import os
import subprocess
import glob
import json
from bot import LOGGER

# ✅ Telegram's real limit (your discovery)
MAX_TELEGRAM_SIZE = 2000 * 1024 * 1024  # 2,097,152,000 bytes
TARGET_MIN_SIZE = 1.95 * 1024 * 1024 * 1024  # 1.95 GB
TARGET_MAX_SIZE = 1.99 * 1024 * 1024 * 1024  # 1.99 GB
MAX_ATTEMPTS = 10  # 10 retries
MIN_SPLIT_SECONDS = 300  # 5 minutes

def get_video_info(filepath):
    cmd = [
        "ffprobe", "-v", "error", "-of", "json",
        "-show_entries", "format=duration,size,bit_rate",
        filepath
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        return json.loads(result.stdout)
    except Exception as e:
        LOGGER.error(f"ffprobe failed: {e}")
        return None

def is_valid_part_size(file_path):
    size = os.path.getsize(file_path)
    return size < MAX_TELEGRAM_SIZE and size >= TARGET_MIN_SIZE

def universal_split_if_needed(file_path: str) -> list:
    file_size = os.path.getsize(file_path)
    if file_size <= MAX_TELEGRAM_SIZE:
        LOGGER.info(f"File ≤2000MiB. No split needed.")
        return [file_path]

    dir_name = os.path.dirname(file_path)
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    LOGGER.info(f"✂️ Splitting {file_path} for Telegram...")

    probe = get_video_info(file_path)
    if not probe or not probe.get("format"):
        LOGGER.warning("ffprobe failed. Using fallback durations.")
        durations = [3600, 3300, 3000, 2700, 2400, 2100, 1800, 1500, 1200, 900]
    else:
        try:
            duration = float(probe["format"]["duration"])
            file_bytes = int(probe["format"]["size"])
            bitrate_bps = int(probe["format"].get("bit_rate") or (file_bytes * 8) / max(duration, 60))

            # Predict durations: from optimistic to conservative
            target_bits = TARGET_MAX_SIZE * 8
            ideal_duration_sec = max(MIN_SPLIT_SECONDS, target_bits / bitrate_bps)
            durations = [int(ideal_duration_sec * (0.95 ** i)) for i in range(MAX_ATTEMPTS)]
            # Add fallbacks if needed
            durations += [3600, 3300, 3000, 2700, 2400]  # Final fallbacks
        except Exception as e:
            LOGGER.error(f"Bitrate estimation failed: {e}")
            durations = [3600, 3300, 3000, 2700, 2400, 2100, 1800, 1500, 1200, 900]

    for attempt, duration_sec in enumerate(durations, 1):
        output_pattern = os.path.join(dir_name, f"{base_name} - Part %03d.mkv")
        # Clean old parts
        for old in glob.glob(output_pattern.replace("%03d", "*")):
            try:
                os.remove(old)
            except:
                pass

        LOGGER.info(f"🔁 Attempt {attempt}/{MAX_ATTEMPTS} | Target: {duration_sec//60} min splits")

        cmd = [
            "ffmpeg", "-i", file_path,
            "-c", "copy", "-map", "0",
            "-f", "segment",
            "-segment_time", str(duration_sec),
            "-reset_timestamps", "1",
            "-avoid_negative_ts", "make_zero",
            "-y", output_pattern
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                LOGGER.warning(f"ffmpeg failed: {result.stderr[:100]}...")
                continue

            split_files = sorted(glob.glob(os.path.join(dir_name, f"{base_name} - Part *.mkv")))
            if len(split_files) < 2:
                LOGGER.warning("Only one part created.")
                continue

            # ✅ Verify all parts < 2000 MiB and ≥1.95 GB (except last)
            oversized = [f for f in split_files if os.path.getsize(f) >= MAX_TELEGRAM_SIZE]
            small_early = [f for f in split_files[:-1] if os.path.getsize(f) < TARGET_MIN_SIZE]

            if not oversized and not small_early:
                LOGGER.info(f"✅ Perfect split! {len(split_files)} parts")
                for f in split_files:
                    gb = os.path.getsize(f) / (1024**3)
                    LOGGER.info(f"   → {os.path.basename(f)} ({gb:.2f} GB)")
                return split_files
            else:
                if oversized:
                    LOGGER.warning(f"❌ {len(oversized)} part(s) ≥2000MiB. Reducing duration...")
                if small_early:
                    LOGGER.warning(f"❌ {len(small_early)} early parts <1.95GB. Increasing duration...")
                continue

        except Exception as e:
            LOGGER.error(f"Split failed: {e}")
            continue

    LOGGER.error("❌ All attempts failed. Falling back to safe 60-min split.")
    output_pattern = os.path.join(dir_name, f"{base_name} - Part %03d.mkv")
    cmd = ["ffmpeg", "-i", file_path, "-c", "copy", "-map", "0", "-f", "segment", "-segment_time", "3600", "-y", output_pattern]
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return sorted(glob.glob(os.path.join(dir_name, f"{base_name} - Part *.mkv")))
