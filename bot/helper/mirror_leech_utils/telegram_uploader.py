import contextlib
from asyncio import sleep
from logging import getLogger
import os
from os import path as ospath
from os import walk
from re import match as re_match
from re import sub as re_sub
from time import time

from aiofiles.os import (
    path as aiopath,
)
from aiofiles.os import (
    remove,
    rename,
)
from aioshutil import rmtree
from natsort import natsorted
from PIL import Image
from pyrogram.errors import BadRequest, FloodPremiumWait, FloodWait, RPCError
from pyrogram.types import (
    InputMediaDocument,
    InputMediaPhoto,
    InputMediaVideo,
)
from tenacity import (
    RetryError,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from bot.core.aeon_client import TgClient
from bot.core.config_manager import Config
from bot.helper.aeon_utils.caption_gen import generate_caption
from bot.helper.ext_utils.bot_utils import sync_to_async
from bot.helper.ext_utils.files_utils import (
    get_base_name,
    is_archive,
)
from bot.helper.ext_utils.media_utils import (
    get_audio_thumbnail,
    get_document_type,
    get_media_info,
    get_multiple_frames_thumbnail,
    get_video_thumbnail,
)
from bot.helper.telegram_helper.message_utils import delete_message

# Import VideoProcessor components
import os
import json
import asyncio
import ffmpeg
import logging
import subprocess
import tempfile
import shutil
import re
import gc
import random

LOGGER = getLogger(__name__)

class VideoProcessor:
    def __init__(self, config_path: str = None):
        # If no config path provided, create a minimal default config
        if config_path and os.path.exists(config_path):
            with open(config_path, 'r') as f:
                self.config = json.load(f)
        else:
            self.config = {}
        
        self.watermark_path = ospath.join(ospath.dirname(__file__), "watermark.png")
        self.cover_path = ospath.join(ospath.dirname(__file__), "cover.png")

        self.language_map = {
            'eng': 'English', 'en': 'English', 'english': 'English',
            'hin': 'Hindi', 'hi': 'Hindi', 'hindi': 'Hindi',
            'spa': 'Spanish', 'es': 'Spanish', 'spanish': 'Spanish',
            'fre': 'French', 'fr': 'French', 'french': 'French',
            'deu': 'German', 'de': 'German', 'german': 'German',
            'ita': 'Italian', 'it': 'Italian', 'italian': 'Italian',
            'por': 'Portuguese', 'pt': 'Portuguese', 'portuguese': 'Portuguese',
            'rus': 'Russian', 'ru': 'Russian', 'russian': 'Russian',
            'jpn': 'Japanese', 'ja': 'Japanese', 'japanese': 'Japanese',
            'kor': 'Korean', 'ko': 'Korean', 'korean': 'Korean',
            'chi': 'Chinese', 'zh': 'Chinese', 'chinese': 'Chinese',
            'ara': 'Arabic', 'ar': 'Arabic', 'arabic': 'Arabic',
            'tur': 'Turkish', 'tr': 'Turkish', 'turkish': 'Turkish',
            'urd': 'Urdu', 'ur': 'Urdu', 'urdu': 'Urdu',
            'ben': 'Bengali', 'bn': 'Bengali', 'bengali': 'Bengali',
            'tam': 'Tamil', 'ta': 'Tamil', 'tamil': 'Tamil',
            'tel': 'Telugu', 'te': 'Telugu', 'telugu': 'Telugu',
            'mar': 'Marathi', 'mr': 'Marathi', 'marathi': 'Marathi',
            'guj': 'Gujarati', 'gu': 'Gujarati', 'gujarati': 'Gujarati',
            'kan': 'Kannada', 'kn': 'Kannada', 'kannada': 'Kannada',
            'mal': 'Malayalam', 'ml': 'Malayalam', 'malayalam': 'Malayalam',
            'pan': 'Punjabi', 'pa': 'Punjabi', 'punjabi': 'Punjabi',
        }

    def _map_language_code(self, code: str) -> str:
        code_lower = code.lower().strip()
        return self.language_map.get(code_lower, code)

    async def _build_stream_metadata_args(self, input_path: str) -> list:
        """Build ffmpeg -metadata arguments for audio/subtitle stream renaming and set English defaults."""
        args = []
        try:
            probe = await asyncio.to_thread(ffmpeg.probe, input_path)
            streams = probe.get('streams', [])

            audio_idx = 0
            subtitle_idx = 0
            eng_audio_index = None
            eng_sub_index = None

            # Identify English audio/subtitle streams
            for stream in streams:
                if stream['codec_type'] == 'audio':
                    lang = stream.get('tags', {}).get('language', '').lower()
                    if lang in ('en', 'eng', 'english'):
                        eng_audio_index = audio_idx
                    audio_idx += 1
                elif stream['codec_type'] == 'subtitle':
                    lang = stream.get('tags', {}).get('language', '').lower()
                    if lang in ('en', 'eng', 'english'):
                        eng_sub_index = subtitle_idx
                    subtitle_idx += 1

            # Reset counters for metadata tagging
            audio_idx = 0
            subtitle_idx = 0

            # Build metadata and disposition args
            for stream in streams:
                if stream['codec_type'] == 'audio':
                    lang = stream.get('tags', {}).get('language', '').strip()
                    lang_name = self._map_language_code(lang) if lang else ""
                    title = f"{lang_name} @paxtv on Telegram" if lang_name else "@paxtv on Telegram"
                    args += [f"-metadata:s:a:{audio_idx}", f"title={title}"]
                    if eng_audio_index is not None:
                        if audio_idx == eng_audio_index:
                            args += [f"-disposition:a:{audio_idx}", "default"]
                        else:
                            args += [f"-disposition:a:{audio_idx}", "0"]
                    audio_idx += 1

                elif stream['codec_type'] == 'subtitle':
                    lang = stream.get('tags', {}).get('language', '').strip()
                    lang_name = self._map_language_code(lang) if lang else ""
                    title = f"{lang_name} @paxtv on Telegram" if lang_name else "@paxtv on Telegram"
                    args += [f"-metadata:s:s:{subtitle_idx}", f"title={title}"]
                    if eng_sub_index is not None:
                        if subtitle_idx == eng_sub_index:
                            args += [f"-disposition:s:{subtitle_idx}", "default"]
                        else:
                            args += [f"-disposition:s:{subtitle_idx}", "0"]
                    subtitle_idx += 1

        except Exception as e:
            LOGGER.warning(f"Failed to build stream metadata args: {e}")
        return args

    async def _finalize_process(self, process: asyncio.subprocess.Process | None):
        try:
            if not process:
                return
            if getattr(process, 'returncode', None) is None:
                try:
                    process.kill()
                except Exception:
                    pass
                try:
                    await process.wait()
                except Exception:
                    pass
            if getattr(process, 'stdout', None):
                try:
                    process.stdout.close()
                except Exception:
                    pass
            if getattr(process, 'stderr', None):
                try:
                    process.stderr.close()
                except Exception:
                    pass
        finally:
            try:
                gc.collect()
            except Exception:
                pass

    async def copy_file_with_thumbnail(self, input_path: str, output_path: str, progress_callback=None) -> bool:
        process = None
        try:
            if progress_callback:
                await progress_callback("📋 Processing video file with thumbnail...")

            if not output_path.endswith('.mkv'):
                output_path = ospath.splitext(output_path)[0] + '.mkv'

            metadata_args = await self._build_stream_metadata_args(input_path)

            cmd = [
                'ffmpeg', '-i', input_path,
                '-map', '0',
                '-c', 'copy',
                '-attach', self.cover_path,
                '-metadata:s:t:0', 'mimetype=image/png',
                *metadata_args,
                '-f', 'matroska', '-y', output_path
            ]

            LOGGER.info(f"Executing FFmpeg command for video processing: {' '.join(cmd)}")
            process = await asyncio.create_subprocess_exec(
                *cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, start_new_session=True
            )

            _, stderr = await process.communicate()

            if process.returncode != 0:
                error_msg = stderr.decode('utf-8', errors='ignore') if stderr else "Unknown error"
                LOGGER.error(f"FFmpeg copy failed with return code {process.returncode}: {error_msg}")
                if progress_callback:
                    await progress_callback("❌ File processing failed")
                return False

            LOGGER.info(f"Successfully processed video file: {input_path} -> {output_path}")
            if progress_callback:
                await progress_callback("✅ File processed with thumbnail!")
            return True
        except Exception as e:
            LOGGER.error(f"Error processing file {input_path}: {e}")
            if progress_callback:
                await progress_callback(f"❌ Processing error: {str(e)}")
            return False
        finally:
            await self._finalize_process(process)


class TelegramUploader:
    def __init__(self, listener, path):
        self._last_uploaded = 0
        self._processed_bytes = 0
        self._listener = listener
        self._user_id = listener.user_id
        self._path = path
        self._start_time = time()
        self._total_files = 0
        self._thumb = self._listener.thumb or f"thumbnails/{listener.user_id}.jpg"
        self._msgs_dict = {}
        self._corrupted = 0
        self._is_corrupted = False
        self._media_dict = {"videos": {}, "documents": {}}
        self._last_msg_in_group = False
        self._up_path = ""
        self._lprefix = ""
        self._user_dump = ""
        self._lcaption = ""
        self._media_group = False
        self._is_private = False
        self._sent_msg = None
        self.log_msg = None
        self._user_session = self._listener.user_transmission
        self._error = ""
        
        # Initialize VideoProcessor
        self.video_processor = VideoProcessor()

    async def _upload_progress(self, current, _):
        if self._listener.is_cancelled:
            if self._user_session:
                TgClient.user.stop_transmission()
            else:
                self._listener.client.stop_transmission()
        chunk_size = current - self._last_uploaded
        self._last_uploaded = current
        self._processed_bytes += chunk_size

    async def _user_settings(self):
        self._media_group = self._listener.user_dict.get("MEDIA_GROUP") or (
            Config.MEDIA_GROUP
            if "MEDIA_GROUP" not in self._listener.user_dict
            else False
        )
        self._lprefix = self._listener.user_dict.get("LEECH_FILENAME_PREFIX") or (
            Config.LEECH_FILENAME_PREFIX
            if "LEECH_FILENAME_PREFIX" not in self._listener.user_dict
            else ""
        )
        self._user_dump = self._listener.user_dict.get("USER_DUMP")
        self._lcaption = self._listener.user_dict.get("LEECH_FILENAME_CAPTION") or (
            Config.LEECH_FILENAME_CAPTION
            if "LEECH_FILENAME_CAPTION" not in self._listener.user_dict
            else ""
        )
        if self._thumb != "none" and not await aiopath.exists(self._thumb):
            self._thumb = None

    async def _msg_to_reply(self):
        if self._listener.up_dest:
            msg = self._listener.message.text.lstrip("/")
            try:
                if self._user_session:
                    self._sent_msg = await TgClient.user.send_message(
                        chat_id=self._listener.up_dest,
                        text=msg,
                        disable_web_page_preview=True,
                        message_thread_id=self._listener.chat_thread_id,
                        disable_notification=True,
                    )
                else:
                    self._sent_msg = await self._listener.client.send_message(
                        chat_id=self._listener.up_dest,
                        text=msg,
                        disable_web_page_preview=True,
                        message_thread_id=self._listener.chat_thread_id,
                        disable_notification=True,
                    )
                    self._is_private = self._sent_msg.chat.type.name == "PRIVATE"
                self.log_msg = self._sent_msg
            except Exception as e:
                await self._listener.on_upload_error(str(e))
                return False
        elif self._user_session:
            self._sent_msg = await TgClient.user.get_messages(
                chat_id=self._listener.message.chat.id,
                message_ids=self._listener.mid,
            )
            if self._sent_msg is None:
                self._sent_msg = await TgClient.user.send_message(
                    chat_id=self._listener.message.chat.id,
                    text="Deleted Cmd Message! Don't delete the cmd message again!",
                    disable_web_page_preview=True,
                    disable_notification=True,
                )
        else:
            self._sent_msg = self._listener.message
        return True

    async def _prepare_file(self, file_, dirpath):
        if self._lcaption:
            cap_mono = await generate_caption(file_, dirpath, self._lcaption)
        if self._lprefix:
            if not self._lcaption:
                cap_mono = f"{self._lprefix} {file_}"
            self._lprefix = re_sub("<.*?>", "", self._lprefix)
            new_path = ospath.join(dirpath, f"{self._lprefix} {file_}")
            LOGGER.info(self._up_path)
            await rename(self._up_path, new_path)
            self._up_path = new_path
            LOGGER.info(self._up_path)
        if not self._lcaption and not self._lprefix:
            cap_mono = f"<code>{file_}</code>"
        if len(file_) > 60:
            if is_archive(file_):
                name = get_base_name(file_)
                ext = file_.split(name, 1)[1]
            elif match := re_match(
                r".+(?=\..+\.0*\d+$)|.+(?=\.part\d+\..+$)",
                file_,
            ):
                name = match.group(0)
                ext = file_.split(name, 1)[1]
            elif len(fsplit := ospath.splitext(file_)) > 1:
                name = fsplit[0]
                ext = fsplit[1]
            else:
                name = file_
                ext = ""
            extn = len(ext)
            remain = 60 - extn
            name = name[:remain]
            new_path = ospath.join(dirpath, f"{name}{ext}")
            await rename(self._up_path, new_path)
            self._up_path = new_path
        return cap_mono

    async def _process_video_file(self, file_path: str) -> str:
        """Process MKV/MP4 files with VideoProcessor and return the processed file path."""
        original_path = file_path
        temp_dir = None
        temp_input_path = None
        processed_temp_path = None

        try:
            if not file_path.lower().endswith(('.mkv', '.mp4')):
                LOGGER.info(f"Skipping non-video file: {file_path}")
                return file_path

            LOGGER.info(f"Intercepting video file for processing: {file_path}")

            # Create a temporary directory for processing
            temp_dir = await asyncio.to_thread(tempfile.mkdtemp)

            # Generate a sanitized filename with random numbers
            random_suffix = ''.join(random.choices('0123456789', k=8))
            sanitized_name = f"Renamed-{random_suffix}{ospath.splitext(file_path)[1]}"
            temp_input_path = ospath.join(temp_dir, sanitized_name)

            LOGGER.info(f"Copying original file to sanitized name: {temp_input_path}")

            # Copy the original file to the temporary location with sanitized name
            await asyncio.to_thread(shutil.copy2, file_path, temp_input_path)

            # Verify the copy was successful
            if not await aiopath.exists(temp_input_path):
                raise Exception("Failed to copy file to temporary location")

            # Create output path in the same temporary directory
            processed_temp_path = ospath.join(temp_dir, f"processed_{sanitized_name}")

            # Define progress callback for video processing
            async def progress_callback(message):
                LOGGER.info(f"Video Processing [{ospath.basename(file_path)}]: {message}")

            # Ensure temp_input_path exists before processing
            if not await aiopath.exists(temp_input_path):
                raise Exception(f"Temp input file missing before processing: {temp_input_path}")

            output_dir = os.path.dirname(processed_temp_path)
            if not await aiopath.exists(output_dir):
                await asyncio.to_thread(os.makedirs, output_dir, exist_ok=True)
                
            # Process the video file using the sanitized temp file
            LOGGER.info(f"Starting video processing with sanitized filename: {temp_input_path} -> {processed_temp_path}")
            success = await self.video_processor.copy_file_with_thumbnail(
                temp_input_path, processed_temp_path, progress_callback
            )

            if success:
                if await aiopath.exists(processed_temp_path):
                    processed_size = await aiopath.getsize(processed_temp_path)
                    original_size = await aiopath.getsize(file_path)
                    LOGGER.info(f"Video processing successful: {file_path} -> {processed_temp_path} "
                               f"(Original: {original_size} bytes, Processed: {processed_size} bytes)")

                    # Remove original file and move processed file to original location
                    await remove(file_path)
                    await rename(processed_temp_path, file_path)
                    LOGGER.info(f"Successfully replaced original file with processed file: {file_path}")

                    # Clean up only after successful replacement
                    if temp_input_path and await aiopath.exists(temp_input_path):
                        await remove(temp_input_path)
                    if temp_dir and await aiopath.exists(temp_dir):
                        try:
                            # Remove temp directory only if empty
                            dir_contents = await asyncio.to_thread(os.listdir, temp_dir)
                            if not dir_contents:
                                await asyncio.to_thread(shutil.rmtree, temp_dir, ignore_errors=True)
                            else:
                                LOGGER.warning(f"Temporary directory not empty after rename: {temp_dir}")
                        except Exception as e:
                            LOGGER.warning(f"Error cleaning up temp directory {temp_dir}: {e}")

                    return file_path
                else:
                    LOGGER.error(f"Processed file not found: {processed_temp_path}")
                    # Cleanup temp files but keep original for fallback
                    if temp_input_path and await aiopath.exists(temp_input_path):
                        await remove(temp_input_path)
                    if processed_temp_path and await aiopath.exists(processed_temp_path):
                        await remove(processed_temp_path)
                    if temp_dir and await aiopath.exists(temp_dir):
                        try:
                            await asyncio.to_thread(shutil.rmtree, temp_dir, ignore_errors=True)
                        except Exception:
                            pass
                    return file_path
            else:
                LOGGER.warning(f"Video processing failed, using original file: {file_path}")
                # Clean up temp input only if exists
                if temp_input_path and await aiopath.exists(temp_input_path):
                    await remove(temp_input_path)
                if processed_temp_path and await aiopath.exists(processed_temp_path):
                    await remove(processed_temp_path)
                if temp_dir and await aiopath.exists(temp_dir):
                    try:
                        await asyncio.to_thread(shutil.rmtree, temp_dir, ignore_errors=True)
                    except Exception:
                        pass
                return file_path

        except Exception as e:
            LOGGER.error(f"Error processing video file {file_path}: {e}")
            # Cleanup any temp files before returning original
            if temp_input_path and await aiopath.exists(temp_input_path):
                await remove(temp_input_path)
            if processed_temp_path and await aiopath.exists(processed_temp_path):
                await remove(processed_temp_path)
            if temp_dir and await aiopath.exists(temp_dir):
                try:
                    await asyncio.to_thread(shutil.rmtree, temp_dir, ignore_errors=True)
                except Exception:
                    pass
            if await aiopath.exists(original_path):
                return original_path
            else:
                # If original was deleted but processing failed, this is critical
                raise Exception(f"Processing failed and original file may be lost: {e}")

    def _get_input_media(self, subkey, key):
        rlist = []
        for msg in self._media_dict[key][subkey]:
            if key == "videos":
                input_media = InputMediaVideo(
                    media=msg.video.file_id,
                    caption=msg.caption,
                )
            else:
                input_media = InputMediaDocument(
                    media=msg.document.file_id,
                    caption=msg.caption,
                )
            rlist.append(input_media)
        return rlist

    async def _send_screenshots(self, dirpath, outputs):
        inputs = [
            InputMediaPhoto(ospath.join(dirpath, p), p.rsplit("/", 1)[-1])
            for p in outputs
        ]
        for i in range(0, len(inputs), 10):
            batch = inputs[i : i + 10]
            self._sent_msg = (
                await self._sent_msg.reply_media_group(
                    media=batch,
                    quote=True,
                    disable_notification=True,
                )
            )[-1]

    async def _send_media_group(self, subkey, key, msgs):
        for index, msg in enumerate(msgs):
            if self._listener.hybrid_leech or not self._user_session:
                msgs[index] = await self._listener.client.get_messages(
                    chat_id=msg[0],
                    message_ids=msg[1],
                )
            else:
                msgs[index] = await TgClient.user.get_messages(
                    chat_id=msg[0],
                    message_ids=msg[1],
                )
        msgs_list = await msgs[0].reply_to_message.reply_media_group(
            media=self._get_input_media(subkey, key),
            quote=True,
            disable_notification=True,
        )
        for msg in msgs:
            if msg.link in self._msgs_dict:
                del self._msgs_dict[msg.link]
            await delete_message(msg)
        del self._media_dict[key][subkey]
        if self._listener.is_super_chat or self._listener.up_dest:
            for m in msgs_list:
                self._msgs_dict[m.link] = m.caption
        self._sent_msg = msgs_list[-1]

    async def upload(self):
        await self._user_settings()
        res = await self._msg_to_reply()
        if not res:
            return
        for dirpath, _, files in natsorted(await sync_to_async(walk, self._path)):
            if dirpath.strip().endswith("/yt-dlp-thumb"):
                continue
            if dirpath.strip().endswith("_ss"):
                await self._send_screenshots(dirpath, files)
                await rmtree(dirpath, ignore_errors=True)
                continue
            for file_ in natsorted(files):
                self._error = ""
                self._up_path = f_path = ospath.join(dirpath, file_)
                if not await aiopath.exists(self._up_path):
                    LOGGER.error(f"{self._up_path} not exists! Continue uploading!")
                    continue
                try:
                    f_size = await aiopath.getsize(self._up_path)
                    self._total_files += 1
                    if f_size == 0:
                        LOGGER.error(
                            f"{self._up_path} size is zero, telegram don't upload zero size files",
                        )
                        self._corrupted += 1
                        continue
                    if self._listener.is_cancelled:
                        return
                    cap_mono = await self._prepare_file(file_, dirpath)
                    
                    # Intercept and process MKV/MP4 files before upload
                    if self._up_path.lower().endswith(('.mkv', '.mp4')):
                        LOGGER.info(f"Intercepting video file for processing: {self._up_path}")
                        # Store original path for reference
                        original_path = self._up_path
                        self._up_path = await self._process_video_file(self._up_path)
                        LOGGER.info(f"Video processing completed. Using file: {self._up_path}")
                    
                    if self._last_msg_in_group:
                        group_lists = [
                            x for v in self._media_dict.values() for x in v
                        ]
                        match = re_match(
                            r".+(?=\.0*\d+$)|.+(?=\.part\d+\..+$)",
                            f_path,
                        )
                        if not match or (
                            match and match.group(0) not in group_lists
                        ):
                            for key, value in list(self._media_dict.items()):
                                for subkey, msgs in list(value.items()):
                                    if len(msgs) > 1:
                                        await self._send_media_group(
                                            subkey,
                                            key,
                                            msgs,
                                        )
                    if (
                        self._listener.hybrid_leech
                        and self._listener.user_transmission
                    ):
                        self._user_session = f_size > 2097152000
                        if self._user_session:
                            self._sent_msg = await TgClient.user.get_messages(
                                chat_id=self._sent_msg.chat.id,
                                message_ids=self._sent_msg.id,
                            )
                        else:
                            self._sent_msg = (
                                await self._listener.client.get_messages(
                                    chat_id=self._sent_msg.chat.id,
                                    message_ids=self._sent_msg.id,
                                )
                            )
                    self._last_msg_in_group = False
                    self._last_uploaded = 0
                    await self._upload_file(cap_mono, file_, f_path)
                    if self._listener.is_cancelled:
                        return
                    if (
                        not self._is_corrupted
                        and (self._listener.is_super_chat or self._listener.up_dest)
                        and not self._is_private
                    ):
                        self._msgs_dict[self._sent_msg.link] = file_
                    await sleep(1)
                except Exception as err:
                    if isinstance(err, RetryError):
                        LOGGER.info(
                            f"Total Attempts: {err.last_attempt.attempt_number}",
                        )
                        err = err.last_attempt.exception()
                    LOGGER.error(f"{err}. Path: {self._up_path}")
                    self._error = str(err)
                    self._corrupted += 1
                    if self._listener.is_cancelled:
                        return
                if not self._listener.is_cancelled and await aiopath.exists(
                    self._up_path,
                ):
                    await remove(self._up_path)
        for key, value in list(self._media_dict.items()):
            for subkey, msgs in list(value.items()):
                if len(msgs) > 1:
                    try:
                        await self._send_media_group(subkey, key, msgs)
                    except Exception as e:
                        LOGGER.info(
                            f"While sending media group at the end of task. Error: {e}",
                        )
        if self._listener.is_cancelled:
            return
        if self._total_files == 0:
            await self._listener.on_upload_error(
                "No files to upload. In case you have filled EXCLUDED_EXTENSIONS, then check if all files have those extensions or not.",
            )
            return
        if self._total_files <= self._corrupted:
            await self._listener.on_upload_error(
                f"Files Corrupted or unable to upload. {self._error or 'Check logs!'}",
            )
            return
        LOGGER.info(f"Leech Completed: {self._listener.name}")
        await self._listener.on_upload_complete(
            None,
            self._msgs_dict,
            self._total_files,
            self._corrupted,
        )
        return

    @retry(
        wait=wait_exponential(multiplier=2, min=4, max=8),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type(Exception),
    )
    async def _upload_file(self, cap_mono, file, o_path, force_document=False):
        if (
            self._thumb is not None
            and not await aiopath.exists(self._thumb)
            and self._thumb != "none"
        ):
            self._thumb = None
        thumb = self._thumb
        self._is_corrupted = False
        try:
            is_video, is_audio, is_image = await get_document_type(self._up_path)

            if not is_image and thumb is None:
                file_name = ospath.splitext(file)[0]
                thumb_path = f"{self._path}/yt-dlp-thumb/{file_name}.jpg"
                if await aiopath.isfile(thumb_path):
                    thumb = thumb_path
                elif is_audio and not is_video:
                    thumb = await get_audio_thumbnail(self._up_path)

            if (
                self._listener.as_doc
                or force_document
                or (not is_video and not is_audio and not is_image)
            ):
                key = "documents"
                if is_video and thumb is None:
                    thumb = await get_video_thumbnail(self._up_path, None)

                if self._listener.is_cancelled:
                    return None
                if thumb == "none":
                    thumb = None
                self._sent_msg = await self._sent_msg.reply_document(
                    document=self._up_path,
                    quote=True,
                    thumb=thumb,
                    caption=cap_mono,
                    force_document=True,
                    disable_notification=True,
                    progress=self._upload_progress,
                )
            elif is_video:
                key = "videos"
                duration = (await get_media_info(self._up_path))[0]
                if thumb is None and self._listener.thumbnail_layout:
                    thumb = await get_multiple_frames_thumbnail(
                        self._up_path,
                        self._listener.thumbnail_layout,
                        self._listener.screen_shots,
                    )
                if thumb is None:
                    thumb = await get_video_thumbnail(self._up_path, duration)
                if thumb is not None and thumb != "none":
                    with Image.open(thumb) as img:
                        width, height = img.size
                else:
                    width = 480
                    height = 320
                if self._listener.is_cancelled:
                    return None
                if thumb == "none":
                    thumb = None
                self._sent_msg = await self._sent_msg.reply_video(
                    video=self._up_path,
                    quote=True,
                    caption=cap_mono,
                    duration=duration,
                    width=width,
                    height=height,
                    thumb=thumb,
                    supports_streaming=True,
                    disable_notification=True,
                    progress=self._upload_progress,
                )
            elif is_audio:
                key = "audios"
                duration, artist, title = await get_media_info(self._up_path)
                if self._listener.is_cancelled:
                    return None
                self._sent_msg = await self._sent_msg.reply_audio(
                    audio=self._up_path,
                    quote=True,
                    caption=cap_mono,
                    duration=duration,
                    performer=artist,
                    title=title,
                    thumb=thumb,
                    disable_notification=True,
                    progress=self._upload_progress,
                )
            else:
                key = "photos"
                if self._listener.is_cancelled:
                    return None
                self._sent_msg = await self._sent_msg.reply_photo(
                    photo=self._up_path,
                    quote=True,
                    caption=cap_mono,
                    disable_notification=True,
                    progress=self._upload_progress,
                )

            await self._copy_message()

            if (
                not self._listener.is_cancelled
                and self._media_group
                and (self._sent_msg.video or self._sent_msg.document)
            ):
                key = "documents" if self._sent_msg.document else "videos"
                if match := re_match(r".+(?=\.0*\d+$)|.+(?=\.part\d+\..+$)", o_path):
                    pname = match.group(0)
                    if pname in self._media_dict[key]:
                        self._media_dict[key][pname].append(
                            [self._sent_msg.chat.id, self._sent_msg.id],
                        )
                    else:
                        self._media_dict[key][pname] = [
                            [self._sent_msg.chat.id, self._sent_msg.id],
                        ]
                    msgs = self._media_dict[key][pname]
                    if len(msgs) == 10:
                        await self._send_media_group(pname, key, msgs)
                    else:
                        self._last_msg_in_group = True

            if (
                self._thumb is None
                and thumb is not None
                and await aiopath.exists(thumb)
            ):
                await remove(thumb)
        except (FloodWait, FloodPremiumWait) as f:
            LOGGER.warning(str(f))
            await sleep(f.value * 1.3)
            if (
                self._thumb is None
                and thumb is not None
                and await aiopath.exists(thumb)
            ):
                await remove(thumb)
            return await self._upload_file(cap_mono, file, o_path)
        except Exception as err:
            if (
                self._thumb is None
                and thumb is not None
                and await aiopath.exists(thumb)
            ):
                await remove(thumb)
            err_type = "RPCError: " if isinstance(err, RPCError) else ""
            LOGGER.error(f"{err_type}{err}. Path: {self._up_path}")
            if isinstance(err, BadRequest) and key != "documents":
                LOGGER.error(f"Retrying As Document. Path: {self._up_path}")
                return await self._upload_file(cap_mono, file, o_path, True)
            raise err

    async def _copy_message(self):
        await sleep(0.5)

        async def _copy(target, retries=2):
            for attempt in range(retries):
                try:
                    msg = await TgClient.bot.get_messages(
                        self._sent_msg.chat.id,
                        self._sent_msg.id,
                    )
                    await msg.copy(target)
                    return
                except Exception as e:
                    LOGGER.error(f"Attempt {attempt + 1} failed: {e} {msg.id}")
                    if attempt < retries - 1:
                        await sleep(0.5)
            LOGGER.error(f"Failed to copy message after {retries} attempts")

        # TODO if self.dm_mode:
        if self._sent_msg.chat.id != self._user_id:
            await _copy(self._user_id)

        if self._user_dump:
            with contextlib.suppress(Exception):
                await _copy(int(self._user_dump))
        if (
            isinstance(Config.LEECH_DUMP_CHAT, list)
            and len(Config.LEECH_DUMP_CHAT) > 1
        ):
            for i in Config.LEECH_DUMP_CHAT[1:]:
                with contextlib.suppress(Exception):
                    await _copy(i)

    @property
    def speed(self):
        try:
            return self._processed_bytes / (time() - self._start_time)
        except Exception:
            return 0

    @property
    def processed_bytes(self):
        return self._processed_bytes

    async def cancel_task(self):
        self._listener.is_cancelled = True
        LOGGER.info(f"Cancelling Upload: {self._listener.name}")
        await self._listener.on_upload_error("your upload has been stopped!")
