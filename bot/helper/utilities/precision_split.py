import os
import subprocess
import glob
import json
from bot import LOGGER

MAX_TELEGRAM_SIZE = 2000 * 1024 * 1024  # 2,097,152,000 bytes
TARGET_MIN_SIZE = 1.95 * 1024 * 1024 * 1024  # 1.95 GB
MAX_ATTEMPTS = 5

def get_video_info(filepath):
    cmd = ["ffprobe", "-v", "error", "-of", "json", "-show_entries", "format=duration,size,bit_rate", filepath]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        return json.loads(result.stdout)
    except Exception as e:
        LOGGER.error(f"ffprobe failed: {e}")
        return None

def precision_split_if_needed(file_path: str) -> list:
    file_size = os.path.getsize(file_path)
    if file_size <= MAX_TELEGRAM_SIZE:
        return [file_path]

    dir_name = os.path.dirname(file_path)
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    probe = get_video_info(file_path)
    if not probe or not probe.get("format"): return fallback_split(file_path)

    try:
        duration_sec = float(probe["format"]["duration"])
        file_size_bytes = int(probe["format"]["size"])
    except: duration_sec, file_size_bytes = 1.0, file_size

    bitrate_bps = int(probe["format"].get("bit_rate") or (file_size_bytes * 8) / max(duration_sec, 60))
    target_bits = 1.98 * 1024 * 1024 * 1024 * 8
    ideal_duration_sec = max(300, target_bits / bitrate_bps)

    for attempt in range(1, MAX_ATTEMPTS + 1):
        output_pattern = os.path.join(dir_name, f"{base_name} - Part %03d.mkv")
        for old in glob.glob(output_pattern.replace("%03d", "*")): os.remove(old) if os.path.exists(old) else None

        num_parts = int(duration_sec / ideal_duration_sec) + 1
        adjusted_duration = duration_sec / max(num_parts - 1, 1)

        cmd = [
            "ffmpeg", "-i", file_path, "-c", "copy", "-map", "0", "-f", "segment",
            "-segment_time", str(int(adjusted_duration)), "-reset_timestamps", "1",
            "-avoid_negative_ts", "make_zero", "-y", output_pattern
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
            if result.returncode != 0: continue

            part_files = sorted(glob.glob(os.path.join(dir_name, f"{base_name} - Part *.mkv")))
            if len(part_files) < 2: continue

            sizes = [os.path.getsize(f) for f in part_files]
            oversized = [s for s in sizes if s >= MAX_TELEGRAM_SIZE]
            small_early = [s for s in sizes[:-1] if s < TARGET_MIN_SIZE]

            if not oversized and not small_early:
                for f in part_files:
                    gb = os.path.getsize(f) / (1024**3)
                    LOGGER.info(f"✅ {os.path.basename(f)} | {gb:.2f} GB")
                return part_files

            ideal_duration_sec = ideal_duration_sec * 0.9 if oversized else ideal_duration_sec * 1.05
        except Exception as e:
            LOGGER.error(f"Split failed: {e}")

    return fallback_split(file_path)

def fallback_split(file_path):
    dir_name = os.path.dirname(file_path)
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    output_pattern = os.path.join(dir_name, f"{base_name} - Part %03d.mkv")
    cmd = ["ffmpeg", "-i", file_path, "-c", "copy", "-map", "0", "-f", "segment", "-segment_time", "3600", "-y", output_pattern]
    subprocess.run(cmd)
    return sorted(glob.glob(os.path.join(dir_name, f"{base_name} - Part *.mkv")))
