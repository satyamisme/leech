import os
import subprocess
import glob
import shlex
from bot import LOGGER

MAX_TELEGRAM_SIZE = 2000 * 1024 * 1024
TARGET_MIN_SIZE = 1950 * 1024 * 1024
TARGET_MAX_SIZE = 1999 * 1024 * 1024
MAX_ATTEMPTS = 10

def smart_split(file_path: str, output_dir: str) -> list:
    """
    Splits a video file into parts that are between TARGET_MIN_SIZE and TARGET_MAX_SIZE.
    Uses a binary search-like approach with mkvmerge to find the optimal split size.
    """
    file_size = os.path.getsize(file_path)
    if file_size <= MAX_TELEGRAM_SIZE:
        LOGGER.info(f"File '{os.path.basename(file_path)}' is {file_size/1024**2:.2f} MB, which is under the 2000MiB limit. No split needed.")
        return [file_path]

    base_name = os.path.splitext(os.path.basename(file_path))[0]
    output_pattern = os.path.join(output_dir, f"{base_name} - Part %03d.mkv")

    # Clean up any previous split attempts for this file
    for old_part in glob.glob(os.path.join(output_dir, f"{base_name} - Part *.mkv")):
        try:
            os.remove(old_part)
            LOGGER.info(f"Removed old split part: {old_part}")
        except OSError as e:
            LOGGER.error(f"Error removing old part {old_part}: {e}")

    low_mb = 1900  # Start binary search from a safe lower bound
    high_mb = 2000 # Upper bound
    best_split_files = None

    LOGGER.info(f"Starting smart split for '{os.path.basename(file_path)}' ({file_size/1024**3:.2f} GB)")

    for attempt in range(MAX_ATTEMPTS):
        if low_mb > high_mb:
            LOGGER.warning("Binary search bounds crossed. Using best found split or failing.")
            break

        current_size_mb = (low_mb + high_mb) // 2
        LOGGER.info(f"🔁 Attempt {attempt + 1}/{MAX_ATTEMPTS}: Trying split size {current_size_mb}M...")

        # Construct and run mkvmerge command
        cmd = [
            "mkvmerge",
            "-o", output_pattern,
            "--split", f"size:{current_size_mb}M",
            file_path
        ]

        try:
            process = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True,
                timeout=900  # 15 minutes timeout
            )
            LOGGER.info(f"mkvmerge command successful for size {current_size_mb}M.")
            LOGGER.debug(f"mkvmerge stdout:\n{process.stdout}")

        except subprocess.CalledProcessError as e:
            LOGGER.error(f"mkvmerge failed with return code {e.returncode} for size {current_size_mb}M.")
            LOGGER.error(f"mkvmerge stderr:\n{e.stderr}")
            # If mkvmerge fails, it often means the size is too large or there's another issue.
            # Let's assume it's too large and try a smaller size.
            high_mb = current_size_mb - 1
            continue
        except subprocess.TimeoutExpired:
            LOGGER.error(f"mkvmerge timed out for size {current_size_mb}M. This may indicate a problem with the file or system.")
            # Assume it's too large to process quickly
            high_mb = current_size_mb - 1
            continue

        split_files = sorted(glob.glob(os.path.join(output_dir, f"{base_name} - Part *.mkv")))

        if not split_files:
            LOGGER.error(f"mkvmerge ran but created no split files for size {current_size_mb}M. This is unexpected.")
            # This is a strange state, let's try a smaller size.
            high_mb = current_size_mb - 1
            continue

        if len(split_files) == 1:
            LOGGER.warning(f"Split attempt with {current_size_mb}M resulted in only one part. This means the split size is larger than the source file size. Adjusting upper bound.")
            high_mb = current_size_mb - 1
            continue

        # Check the size of the first part to guide the binary search
        first_part_size = os.path.getsize(split_files[0])
        LOGGER.info(f"First part size is {first_part_size / 1024**2:.2f} MB.")

        # Check if all parts (except the last) are within the target range
        is_perfect = True
        all_under_limit = True
        for i, f in enumerate(split_files):
            size = os.path.getsize(f)
            if size > MAX_TELEGRAM_SIZE:
                all_under_limit = False
            # For all but the last part, check if it's in the ideal range
            if i < len(split_files) - 1 and not (TARGET_MIN_SIZE <= size <= TARGET_MAX_SIZE):
                is_perfect = False

        if is_perfect:
            LOGGER.info(f"✅ Perfect split found! Size {current_size_mb}M resulted in parts within the desired {TARGET_MIN_SIZE/1024**2:.0f}-{TARGET_MAX_SIZE/1024**2:.0f} MB range.")
            return split_files

        if all_under_limit:
             # This is a safe split, though not perfect. Store it as a fallback.
            best_split_files = split_files
            LOGGER.info(f"Found a safe (but not perfect) split with size {current_size_mb}M. Will continue searching for a better one.")

        # Adjust binary search bounds based on the first part's size
        if first_part_size > TARGET_MAX_SIZE:
            # First part is too big, need to aim for a smaller size
            high_mb = current_size_mb - 1
            LOGGER.info("First part is too big. Decreasing high bound.")
        else:
            # First part is too small, need to aim for a larger size
            low_mb = current_size_mb + 1
            LOGGER.info("First part is too small. Increasing low bound.")

    if best_split_files:
        LOGGER.warning(f"Could not find a perfect split after {MAX_ATTEMPTS} attempts. Using the best safe split found.")
        return best_split_files

    LOGGER.error("❌ All mkvmerge attempts failed to produce a valid or safe split. Returning original file.")
    return [file_path]
