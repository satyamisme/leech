import os
import glob
import shutil
from bot import LOGGER
from bot.helper.ext_utils.bot_utils import cmd_exec

# Constants for splitting
MAX_TELEGRAM_SIZE = 2000 * 1024 * 1024  # 2,097,152,000 bytes (2000 MiB)
STEP_SIZE_BYTES = 5 * 1024 * 1024      # 5 MiB step for retries
MAX_SPLIT_SIZE_BYTES = 1999 * 1024 * 1024 # The absolute maximum split size (1999 MiB)

def get_file_size(file_path):
    """Safely get the size of a file."""
    try:
        return os.path.getsize(file_path)
    except OSError as e:
        LOGGER.error(f"Could not get size of file {file_path}: {e}")
        return 0

async def split_video_if_needed(file_path: str) -> list:
    """
    Splits a video file using ONLY mkvmerge with a dynamic tiered retry strategy.

    Strategy:
    1. Start with a narrow optimal range (1990-1999 MiB).
    2. If no valid split is found, lower the minimum split size by 5 MiB.
    3. Repeat until the minimum split size reaches 1950 MiB.

    This ensures a systematic search from a narrow optimal range to a wider safe range.
    Returns a list of split file paths, or a list containing the original file if splitting fails.
    """
    if not shutil.which('mkvmerge'):
        LOGGER.error("mkvmerge is not installed.")
        return [file_path]

    original_size = get_file_size(file_path)
    if original_size == 0:
        return []
    if original_size <= MAX_TELEGRAM_SIZE:
        LOGGER.info("File is already within the 2000MiB limit. No split needed.")
        return [file_path]

    dir_name = os.path.dirname(file_path)
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    output_pattern = f"{os.path.join(dir_name, base_name)} - Part %03d.mkv"

    # Define the dynamic strategy parameters
    initial_min_mib = 1990  # Start the search at 1990 MiB
    final_min_mib = 1950    # Stop the search if min goes below 1950 MiB
    min_bytes = initial_min_mib * 1024 * 1024
    step_mib = 5            # Reduce the minimum by 5 MiB on each retry

    LOGGER.info(f"Starting dynamic split strategy. Target range: {initial_min_mib}MiB to 1999MiB, stepping down to {final_min_mib}MiB.")

    while min_bytes >= final_min_mib * 1024 * 1024:
        current_size_bytes = MAX_SPLIT_SIZE_BYTES  # Always start from the highest possible size

        while current_size_bytes >= min_bytes:
            current_mib = current_size_bytes // (1024 * 1024)
            LOGGER.info(f"✂️ Trying split size: {current_mib}MiB (Min: {min_bytes//(1024*1024)}MiB)")

            # Clean up any files from previous attempts
            for old_file in glob.glob(output_pattern.replace("%03d", "*")):
                try:
                    os.remove(old_file)
                except:
                    pass

            # Build and execute the mkvmerge command
            cmd = ["mkvmerge", "-o", output_pattern, "--split", f"size:{current_size_bytes}", file_path]
            _, stderr, return_code = await cmd_exec(cmd)

            if return_code != 0:
                LOGGER.warning(f"mkvmerge failed at {current_mib}MiB. Stderr: {stderr[:100]}...")
                current_size_bytes -= STEP_SIZE_BYTES
                continue

            # Gather the created split files
            split_files = sorted(glob.glob(output_pattern.replace("%03d", "*")))
            if len(split_files) <= 1:
                LOGGER.warning(f"No split occurred at {current_mib}MiB.")
                current_size_bytes -= STEP_SIZE_BYTES
                continue

            # Check every part for the size limit
            oversized = [f for f in split_files if get_file_size(f) >= MAX_TELEGRAM_SIZE]
            if not oversized:
                LOGGER.info(f"✅ Success! Split created {len(split_files)} parts using {current_mib}MiB split size.")
                return split_files
            else:
                LOGGER.warning(f"❌ Part(s) oversized at {current_mib}MiB. Retrying with smaller size.")
                current_size_bytes -= STEP_SIZE_BYTES

        # If we exit the inner loop, this tier has failed. Lower the minimum and try again.
        LOGGER.warning(f"⚠️ No valid split found with minimum {min_bytes//(1024*1024)}MiB. Lowering minimum split size...")
        min_bytes -= step_mib * 1024 * 1024  # Reduce the minimum by 'step_mib' MiB

    # All dynamic attempts have been exhausted
    LOGGER.error("❌ All dynamic mkvmerge attempts failed. Cannot create valid split parts.")
    return [file_path]
