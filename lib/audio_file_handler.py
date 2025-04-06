import json
import logging
import os
import re
import shutil
import subprocess
from datetime import datetime, timezone

module_logger = logging.getLogger('icad_tr_consumer.audio_file_handler')


def convert_epoch_to_iso(epoch_timestamp) -> str:
    return datetime.fromtimestamp(epoch_timestamp, tz=timezone.utc).isoformat()


def save_temporary_json_file(tmp_path, call_data):
    try:
        json_path = os.path.join(tmp_path, call_data["filename"]).replace(".wav", ".json")

        # Writing call data to JSON file
        with open(json_path, "w") as json_file:
            json.dump(call_data, json_file, indent=4)
        module_logger.debug(f"JSON file saved successfully at {json_path}")
    except Exception as e:
        module_logger.error(f"Failed to save JSON file at {json_path}: {e}")
        raise  # Re-raise the exception to handle it in a higher-level function


def save_temporary_wav_file(tmp_path, wav_data, call_data):
    try:
        wav_path = os.path.join(tmp_path, call_data.get("filename", "unknown.wav"))

        # Writing WAV data to file
        with open(wav_path, "wb") as wav_file:
            wav_file.write(wav_data)
        module_logger.debug(f"WAV file saved successfully at {wav_path}")
    except Exception as e:
        module_logger.error(f"Failed to save WAV file at {wav_path}: {e}")
        raise  # Re-raise the exception to handle it in a higher-level function


def save_temporary_files(tmp_path, wav_data, call_data):
    try:
        save_temporary_wav_file(tmp_path, wav_data, call_data)
        save_temporary_json_file(tmp_path, call_data)
        module_logger.debug(f"Temporary Files Saved to {tmp_path}")
        return True
    except OSError as e:
        if e.errno == 28:
            module_logger.error(f"Failed to write temp files to {tmp_path}. No space left on device to write files")
        else:
            module_logger.error(f"Failed to write files to {tmp_path}. OS error occurred: {e}")
        return False
    except Exception as e:
        module_logger.error(f"An unexpected error occurred while writing temporary files to {tmp_path}: {e}")
        return False


def clean_temp_files(tmp_path, call_data):
    wav_path = os.path.join(tmp_path, call_data.get("filename", "unknown.wav"))
    json_path = wav_path.replace(".wav", ".json")
    m4a_path = wav_path.replace(".wav", ".m4a")

    if os.path.isfile(wav_path):
        os.remove(wav_path)

    if os.path.isfile(m4a_path):
        os.remove(m4a_path)

    if os.path.isfile(json_path):
        os.remove(json_path)


def compress_wav_m4a(compression_config, wav_file_path, call_data) -> bool:
    """
    Compress a WAV file to M4A using ffmpeg with optional two-pass loudnorm.
    Embeds metadata using values from call_data.

    :param wav_file_path: Path to the input WAV file
    :param compression_config: Dictionary with compression config:
        {
            "enabled": True/False,
            "m4a_sample_rate": 16000,
            "m4a_bitrate": 96,
            "normalization": True/False,
            "use_loudnorm": True/False,
            "loudnorm_params": {
                "I": -16.0,
                "TP": -1.5,
                "LRA": 11.0
            }
        }
    :param call_data: Dictionary with metadata fields (e.g. talkgroup_group, short_name, etc.)
    :return: True if successful, False otherwise.
    """

    # Check if the WAV file exists
    if not os.path.isfile(wav_file_path):
        module_logger.error(f"WAV file does not exist: {wav_file_path}")
        return False

    # Check ffmpeg availability
    if shutil.which("ffmpeg") is None:
        module_logger.error("ffmpeg is not installed or not found in PATH.")
        return False

    module_logger.info(
        f'Converting WAV to M4A at {compression_config.get("m4a_sample_rate")}@{compression_config.get("m4a_bitrate", 96)}')

    if 'start_time' in call_data:
        call_data['date'] = convert_epoch_to_iso(call_data['start_time'])

    # Create M4A Filename
    m4a_file_path = wav_file_path.replace('.wav', '.m4a')

    #Get config settings
    sample_rate  = compression_config.get("m4a_sample_rate", 16000)
    bitrate      = compression_config.get("m4a_bitrate", 96)
    normalization = compression_config.get("normalization", False)
    use_loudnorm  = compression_config.get("use_loudnorm", False)
    loudnorm_params = compression_config.get("loudnorm_params", {})

    # Prepare metadata arguments
    meta_args = [
        "-metadata", f"album={call_data.get('talkgroup_group', '')} - {call_data.get('short_name', '')}",
        "-metadata", f"artist={call_data.get('talkgroup_group_tag', '')}",
        "-metadata", f"date={call_data.get('date', '')}",
        "-metadata", f"genre={call_data.get('talkgroup_group_tag', '')}",
        "-metadata", f"title={call_data.get('talkgroup_description', '')}",
        "-metadata",
        (
            f"comment=Frequency: {call_data.get('freq', '')}, "
            f"Frequency Error: {call_data.get('freq_error', '')}, "
            f"Signal: {call_data.get('signal', '')}, "
            f"Noise: {call_data.get('noise', '')}, "
            f"Call Length: {call_data.get('call_length', '')} seconds"
        ),
        "-movflags", "frag_keyframe+empty_moov"
    ]

    # ---------------------------------------------
    # If normalization + loudnorm => two-pass
    # ---------------------------------------------
    if normalization and use_loudnorm:
        module_logger.info(
            f"Converting WAV to M4A (two-pass loudnorm) at {sample_rate} Hz / {bitrate}k bitrate"
        )

        # Merge with default loudnorm params if not specified
        loudnorm_defaults = {"I": -16.0, "TP": -1.5, "LRA": 11.0}
        for k, v in loudnorm_defaults.items():
            loudnorm_params.setdefault(k, v)

        # ---------------------------
        # First pass: measure stats
        # ---------------------------
        first_pass_filter_parts = [f"{k}={v}" for k, v in loudnorm_params.items()]
        # Add print_format=json to retrieve stats
        first_pass_filter_str = "loudnorm=" + ":".join(first_pass_filter_parts) + ":print_format=json"

        pass1_command = [
            "ffmpeg",
            "-hide_banner",
            "-y",
            "-i", wav_file_path,
            "-af", first_pass_filter_str,
            "-vn",
            "-sn",
            "-f", "null",
            "-"
        ]
        module_logger.debug(f"First pass command: {' '.join(pass1_command)}")

        try:
            pass1_proc = subprocess.run(
                pass1_command,
                capture_output=True,
                text=True,
                check=True
            )
        except subprocess.CalledProcessError as e:
            error_msg = (
                f"First pass ffmpeg failed. Command: {' '.join(pass1_command)}\n"
                f"Output: {e.output}\nError: {e.stderr}"
            )
            module_logger.error(error_msg)
            return False

        pass1_stderr = pass1_proc.stderr
        match = re.search(r"\{.*?\}", pass1_stderr, flags=re.DOTALL)
        if not match:
            module_logger.error("No loudnorm JSON found in first pass FFmpeg output.")
            return False

        stats = json.loads(match.group(0))

        # The 'offset' might not be in the JSON so we parse it from text
        offset_match = re.search(r"offset\s*:\s*([-\d\.]+)", pass1_stderr)
        offset_val = float(offset_match.group(1)) if offset_match else 0.0

        # ---------------------------
        # Second pass: apply stats
        # ---------------------------
        second_pass_filter_parts = []
        for k, v in loudnorm_params.items():
            # skip print_format
            if k.lower() != "print_format":
                second_pass_filter_parts.append(f"{k}={v}")

        second_pass_filter_parts += [
            f"measured_I={stats['input_i']}",
            f"measured_TP={stats['input_tp']}",
            f"measured_LRA={stats['input_lra']}",
            f"measured_thresh={stats['input_thresh']}",
            f"offset={offset_val}",
            "print_format=summary"
        ]
        second_pass_filter_str = "loudnorm=" + ":".join(second_pass_filter_parts)

        pass2_command = [
                            "ffmpeg",
                            "-hide_banner",
                            "-y",
                            "-i", wav_file_path,
                            "-af", second_pass_filter_str,
                            "-ar", str(sample_rate),
                            "-ac", "1",            # Force mono if desired
                            "-c:a", "aac",
                            "-b:a", f"{bitrate}k",
                            "-vn",
                            "-sn"
                        ] + meta_args + [m4a_file_path]

        module_logger.debug(f"Second pass command: {' '.join(pass2_command)}")

        try:
            completed_process = subprocess.run(
                pass2_command,
                capture_output=True,
                text=True,
                check=True
            )
        except subprocess.CalledProcessError as e:
            error_msg = (
                f"Second pass ffmpeg failed. Command: {' '.join(pass2_command)}\n"
                f"Output: {e.output}\nError: {e.stderr}"
            )
            module_logger.error(error_msg)
            return False

        module_logger.debug(f"Two-pass ffmpeg output: {completed_process.stdout}")
        module_logger.debug(f"Two-pass ffmpeg errors: {completed_process.stderr}")
        module_logger.info(f"Successfully compressed '{wav_file_path}' to '{m4a_file_path}' with two-pass loudnorm.")
        return True

    else:
        # ---------------------------------------------
        # Single pass (no loudnorm) path
        # ---------------------------------------------
        module_logger.info(
            f"Converting WAV to M4A (single pass) at {sample_rate} Hz / {bitrate}k bitrate"
        )

        command = [
                      "ffmpeg",
                      "-hide_banner",
                      "-y",
                      "-i", wav_file_path,
                      "-ar", str(sample_rate),
                      "-ac", "1",
                      "-c:a", "aac",
                      "-b:a", f"{bitrate}k"
                  ] + meta_args + [m4a_file_path]

        module_logger.debug(f"Single pass command: {' '.join(command)}")

        try:
            completed_process = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=True
            )
        except subprocess.CalledProcessError as e:
            error_msg = (
                f"ffmpeg single-pass compression failed.\n"
                f"Command: {' '.join(command)}\n"
                f"Output: {e.output}\nError: {e.stderr}"
            )
            module_logger.error(error_msg)
            return False
        except Exception as ex:
            module_logger.error(f"An unexpected error occurred: {ex}")
            return False

        module_logger.debug(f"Single pass ffmpeg output: {completed_process.stdout}")
        module_logger.debug(f"Single pass ffmpeg errors: {completed_process.stderr}")
        module_logger.info(f"Successfully compressed '{wav_file_path}' to '{m4a_file_path}' without loudnorm.")
        return True


def compress_wav_mp3(compression_config, wav_file_path, call_data):
    # Check if the WAV file exists
    if not os.path.isfile(wav_file_path):
        module_logger.error(f"WAV file does not exist: {wav_file_path}")
        return False

    module_logger.info(
        f'Converting WAV to MP3 at {compression_config.get("mp3_sample_rate")}@{compression_config.get("mp3_bitrate", 96)}')

    if 'start_time' in call_data:
        call_data['date'] = convert_epoch_to_iso(call_data['start_time'])

    # Construct the ffmpeg command
    mp3_file_path = wav_file_path.replace('.wav', '.mp3')

    command = [
        "ffmpeg", "-y", "-i", wav_file_path,
        "-ar", f"{compression_config.get('mp3_sample_rate', 16000)}",
        "-c:a", "libmp3lame", "-ac", "2", "-b:a", f"{compression_config.get('mp3_bitrate', 96)}k",
        "-metadata", f"album={call_data.get('talkgroup_group', '')} - {call_data.get('short_name', '')}",
        "-metadata", f"artist={call_data.get('talkgroup_group_tag', '')}",
        "-metadata", f"date={call_data.get('date', '')}",
        "-metadata", f"genre={call_data.get('talkgroup_group_tag', '')}",
        "-metadata", f"title={call_data.get('talkgroup_description', '')}",
        "-metadata",
        f"comment=Frequency: {call_data.get('freq', '')}, Frequency Error: {call_data.get('freq_error', '')}, Signal: {call_data.get('signal', '')}, Noise: {call_data.get('noise', '')}, Call Length: {call_data.get('call_length', '')} seconds",
        mp3_file_path
    ]

    if compression_config.get('use_loudnorm', False):
        command.insert(4, "-af")
        command.insert(5, "loudnorm=I=-16:TP=-1.5:LRA=11")

    try:
        # Execute the ffmpeg command
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        module_logger.debug(f"ffmpeg output: {result.stdout}")
        module_logger.info(f"Successfully converted WAV to MP3 for file: {wav_file_path}")
        return True
    except subprocess.CalledProcessError as e:
        error_message = f"Failed to convert WAV to MP3 for file {wav_file_path}. Error: {e}"
        module_logger.error(error_message)
        return False
    except Exception as e:
        error_message = f"An unexpected error occurred during conversion of {wav_file_path}: {e}"
        module_logger.error(error_message)
        return False
