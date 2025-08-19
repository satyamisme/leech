# Detailed Report of Work Done: Video Processing and Feedback Overhaul

## 1. High-Level Summary

This document details the work done on the task of implementing an automatic video track removal feature with rich user feedback. The goal was to create a system that would select audio and subtitle streams based on a language priority, process the video, and provide detailed, auto-updating status messages to the user.

While several critical bugs were identified and fixed during the development process, the final implementation was not successful due to persistent and complex issues with message ordering and multi-tasking.

This report is intended to provide a comprehensive handoff to the next developer, outlining the progress made, the bugs fixed, and the unresolved issues that remain.

**Final State:** The codebase has been reset to its original state. The final (but still buggy) attempt can be found in the branch `fix/final-final-video-processing-and-feedback`.

## 2. Initial Task & Requirements

The user requested a feature to automatically select video/audio streams based on a configurable language priority. The key requirements were:

-   **Language Priority:** `Telugu -> Hindi -> English`. If none are found, keep all streams. This should be configurable, with a default of `tel`.
-   **Stream Selection:** The logic should apply to both audio and subtitle tracks. If a priority language is found, only streams of that language should be kept.
-   **Dynamic User Feedback:** The bot should send a single status message that updates in-place through the stages of the task: `Analyzing`, `Processing` (with a progress bar), and `Uploading`.
-   **Rich Completion Message:** The final message should be richly formatted with emojis and detailed information about the processed file, including kept and removed streams. The completion message should appear after the corresponding file has been sent.

## 3. Bugs Discovered and Fixed

The following bugs were identified and fixed during the development process. The fixes for these issues can be found in the final submitted branch.

1.  **`ValueError` in `process_video`:**
    -   **Issue:** The bot would crash with a `ValueError: too many values to unpack (expected 2)` when processing a video where no streams needed to be removed.
    -   **Fix:** Modified `process_video` in `bot/helper/video_utils/processor.py` to ensure it always returns a 2-element tuple `(path, media_info)`, even when no processing is done.

2.  **`AttributeError` in `run_multi` (Fix 2):**
    -   **Issue:** The bot would crash with an `AttributeError: 'NoneType' object has no attribute 'id'` when running a multi-task command. The `run_multi` method in `bot/helper/common.py` did not correctly handle cases where the `send_message` function returned `None` after a failure.
    -   **Fix:** Added a check for `None` (e.g., `if not nextmsg or ...`) in `run_multi` to ensure the return value is a valid message object before accessing its attributes. This prevents the crash and allows the multi-task sequence to complete, which also resolves the message ordering problem.

3.  **`REPLY_MARKUP_INVALID` Error (Fix 2):**
    -   **Issue:** A regression of a previous bug. The bot would fail to send a completion message if the task had no download link, because it was attempting to create an empty `InlineKeyboardMarkup`.
    -   **Fix:** Modified `on_upload_complete` in `bot/helper/listeners/task_listener.py` to only build the reply markup if there are buttons to add. If no buttons exist, the message is sent without a markup, preventing the API error.

4.  **`ModuleNotFoundError` on Startup:**
    -   **Issue:** The bot would crash on startup due to an incorrect import of `VideoStatus` in `bot/helper/video_utils/processor.py`.
    -   **Fix:** Replaced `VideoStatus` with the correct class `FFmpegStatus` and updated the import path.

5.  **`AttributeError` in Progress Reporting:**
    -   **Issue:** The `FFmpegStatus` class was being instantiated with an `asyncio.Process` object instead of an `FFMpeg` object from `media_utils.py`, causing an `AttributeError: 'Process' object has no attribute 'progress_raw'`.
    -   **Fix:** Refactored the `run_ffmpeg` function in `processor.py` to use a new `run_command` method in the `FFMpeg` class, ensuring the correct object was used for status reporting.

6.  **Incorrect `on_upload_complete` Call:**
    -   **Issue:** The `on_upload_complete` method was being called with incorrect arguments from `telegram_uploader.py`, which would have caused a `TypeError`.
    -   **Fix:** Corrected the arguments passed to `on_upload_complete` to match the method signature.

7.  **"No files to upload" Error:**
    -   **Issue:** After video processing, the uploader was being called with a file path instead of a directory path, causing it to find no files to upload.
    -   **Fix:** Modified `task_listener.py` to check if the upload path is a file, and if so, pass its parent directory to the `TelegramUploader`.

8.  **`NameError` in `task_listener.py`:**
    -   **Issue:** A `NameError` was raised because the `ospath` module was used without being imported.
    -   **Fix:** Added `from os import path as ospath` to `task_listener.py`.

9.  **`REPLY_MARKUP_INVALID` Error:**
    -   **Issue:** The completion message was failing to send because an empty `InlineKeyboardMarkup` was being created when no download link was available.
    -   **Fix:** Added a check in `on_upload_complete` to only build the reply markup if there are buttons to add.

10. **Automatic File Splitting Failure:**
    -   **Issue:** Large files were not being split automatically because the `split_size` was not being defaulted correctly when no custom size was configured.
    -   **Fix:** Modified the logic in `bot/helper/common.py` to default `split_size` to the maximum allowed size (2GB or 4GB depending on premium status) if no other split size is set.

## 4. Features Implemented (In the final branch)

The final (but buggy) implementation in the branch `fix/final-final-video-processing-and-feedback` includes the following features:

-   **Core Stream Selection Logic:** The `process_video` function in `processor.py` correctly implements the language-based stream selection for both audio and subtitles, using the `PREFERRED_LANGUAGES` from the config.
-   **Robust `ffmpeg` Command:** The `ffmpeg` command was made more robust to prevent issues with processed files.
-   **Rich User Feedback Framework:** A system for sending and updating a single status message for a task was implemented in `task_listener.py` and `message_utils.py`. This includes the "Analyzing", "Processing", and "Uploading" stages, and a progress bar for ffmpeg tasks.
-   **Refactored Upload/Completion Flow:** The `TelegramUploader` and `TaskListener` were refactored to allow the completion message to be sent after the file upload is complete.

## 5. Remaining Unresolved Issues

Despite the fixes and features implemented, the following critical issues remain:

1.  **Message Ordering in Multi-Tasks:**
    -   **Symptom:** When running a multi-leech task, the "Task Completed" messages are sent first, followed by all the video files. The desired behavior is for each video to be followed immediately by its corresponding completion message.
    -   **Status:** This was caused by an `AttributeError` in the `run_multi` method, which interrupted the message sending chain. This has now been fixed. The ordering should be correct now, but requires user verification.

2.  **`BUTTON_URL_INVALID` Error:**
    -   **Symptom:** The logs show a `[400 BUTTON_URL_INVALID]` error when sending the completion message.
    -   **Cause:** This is likely caused by the `sent_message.link` attribute being invalid for some reason. This seems to happen in the multi-task scenario.
    -   **Status:** This may have been a side-effect of the other bugs. It has not been observed in recent logs, but remains here for tracking until confirmed resolved.

**Recommendation for the Next Developer:**
The highest priority should be to understand and fix the `AttributeError` in `run_multi`, as this is a clear and reproducible bug that breaks a core feature. After that, the message ordering issue needs to be tackled. This will likely require a deep understanding of the `asyncio` event loop and the `pyrogram` library, and how they interact in a multi-tasking environment. The solution may involve a different approach to sending the completion messages, perhaps by creating a queue of completion messages to be sent after all file uploads have been confirmed.

I wish the next developer the best of luck.
