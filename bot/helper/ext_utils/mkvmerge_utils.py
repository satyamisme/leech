import os
import subprocess
import glob
import shutil
from bot import LOGGER

# Constants for splitting
MAX_TELEGRAM_SIZE = 2000 * 1024 * 1024  # 2,097,152,000 bytes (2000 MiB)
# Define the optimal range for split size (1990MiB to 1999MiB)
MIN_SPLIT_SIZE_BYTES = 1990 * 1024 * 1024
MAX_SPLIT_SIZE_BYTES = 1999 * 1024 * 1024
STEP_SIZE_BYTES = 5 * 1024 * 1024  # 5 MiB step for retries

def get_file_size(file_path):
    """Safely get the size of a file."""
    try:
        return os.path.getsize(file_path)
    except OSError as e:
        LOGGER.error(f"Could not get size of file {file_path}: {e}")
        return 0

def smart_split_if_needed(file_path: str) -> list:
    """
    Splits a video file using ONLY mkvmerge.
    It will retry with split sizes from 1999MiB down to 1990MiB (in 5MiB steps)
    until all parts are under 2000MiB.
    Returns a list of split file paths, or a list containing the original file if splitting fails.
    """
    if not shutil.which('mkvmerge'):
        LOGGER.error("mkvmerge is not installed. Splitting failed.")
        return [file_path]
    original_size = get_file_size(file_path)
    if original_size == 0:
        return []

    # If the file is already small enough, no split is needed.
    if original_size <= MAX_TELEGRAM_SIZE:
        LOGGER.info(f"File ≤ 2000MiB. No split needed.")
        return [file_path]

    dir_name = os.path.dirname(file_path)
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    output_pattern = os.path.join(dir_name, f"{base_name} - Part %03d.mkv")

    # Start from the highest possible size (1999MiB) and work down to 1990MiB
    current_split_size_bytes = MAX_SPLIT_SIZE_BYTES

    while current_split_size_bytes >= MIN_SPLIT_SIZE_BYTES:
        current_split_size_mib = current_split_size_bytes // (1024 * 1024)
        LOGGER.info(f"✂️ Attempting to split {file_path} with mkvmerge at {current_split_size_mib}MiB...")

        # Clean up any leftover files from previous attempts
        for old_part in glob.glob(output_pattern.replace("%03d", "*")):
            try:
                os.remove(old_part)
                LOGGER.debug(f"Removed old split part: {old_part}")
            except OSError as e:
                LOGGER.warning(f"Failed to remove old part {old_part}: {e}")

        # Build the mkvmerge command using bytes for maximum precision
        mkvmerge_cmd = [
            "mkvmerge",
            "-o", output_pattern,
            "--split", f"size:{current_split_size_bytes}",
            file_path
        ]

        try:
            # Run mkvmerge
            result = subprocess.run(
                mkvmerge_cmd,
                capture_output=True,
                text=True,
                timeout=600  # 10 minutes timeout
            )

            # Check if mkvmerge executed successfully
            if result.returncode != 0:
                LOGGER.warning(f"mkvmerge failed with return code {result.returncode}. Stderr: {result.stderr[:200]}...")
                # On command failure, reduce the size and retry
                current_split_size_bytes -= STEP_SIZE_BYTES
                continue

            # Gather the output files
            split_files = sorted(glob.glob(os.path.join(dir_name, f"{base_name} - Part *.mkv")))

            # If no split occurred (only one file), or no files were created, retry with a smaller size.
            if len(split_files) <= 1:
                LOGGER.warning(f"mkvmerge created {len(split_files)} part(s). No split occurred. Retrying with smaller size.")
                current_split_size_bytes -= STEP_SIZE_BYTES
                continue

            # Check EVERY part, including the final one, for size
            oversized_parts = [f for f in split_files if get_file_size(f) >= MAX_TELEGRAM_SIZE]

            if not oversized_parts:
                # Success! All parts are within the 2000 MiB limit.
                final_size_mib = get_file_size(split_files[-1]) // (1024 * 1024)
                LOGGER.info(f"✅ Split successful at {current_split_size_mib}MiB! Created {len(split_files)} parts. Final part: {final_size_mib}MiB.")
                return split_files
            else:
                # Some parts are still oversized. Log and retry with a smaller split size.
                LOGGER.warning(f"❌ Found {len(oversized_parts)} oversized part(s) at {current_split_size_mib}MiB. "
                             f"Retrying with {current_split_size_bytes - STEP_SIZE_BYTES} bytes.")
                current_split_size_bytes -= STEP_SIZE_BYTES

        except subprocess.TimeoutExpired:
            LOGGER.error(f"mkvmerge timed out while splitting {file_path}.")
            break
        except Exception as e:
            LOGGER.error(f"An unexpected error occurred while running mkvmerge: {e}")
            break

    # If we exit the loop, all attempts in the 1990-1999MiB range have failed.
    LOGGER.error(f"❌ All mkvmerge attempts (1990-1999MiB) failed. Could not split {file_path} into valid parts.")
    return [file_path]
