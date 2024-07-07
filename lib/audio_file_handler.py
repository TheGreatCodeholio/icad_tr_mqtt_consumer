import json
import logging
import os
import subprocess
from datetime import datetime, timezone

module_logger = logging.getLogger('icad_tr_consumer.audio_file_handler')


def convert_epoch_to_iso(epoch_timestamp):
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


def compress_wav_m4a(compression_config, wav_file_path, call_data):
    # Check if the WAV file exists
    if not os.path.isfile(wav_file_path):
        module_logger.error(f"WAV file does not exist: {wav_file_path}")
        return False

    module_logger.info(
        f'Converting WAV to M4A at {compression_config.get("m4a_sample_rate")}@{compression_config.get("m4a_bitrate", 96)}')

    if 'start_time' in call_data:
        call_data['date'] = convert_epoch_to_iso(call_data['start_time'])

    # Construct the ffmpeg command
    m4a_file_path = wav_file_path.replace('.wav', '.m4a')

    command = [
        "ffmpeg", "-y", "-i", wav_file_path,
        "-ar", f"{compression_config.get('m4a_sample_rate', 16000)}",
        "-c:a", "aac", "-ac", "1", "-b:a", f"{compression_config.get('m4a_bitrate', 96)}k",
        "-metadata", f"album={call_data.get('talkgroup_group', '')} - {call_data.get('short_name', '')}",
        "-metadata", f"artist={call_data.get('talkgroup_group_tag', '')}",
        "-metadata", f"date={call_data.get('date', '')}",
        "-metadata", f"genre={call_data.get('talkgroup_group_tag', '')}",
        "-metadata", f"title={call_data.get('talkgroup_description', '')}",
        "-metadata",
        f"comment=Frequency: {call_data.get('freq', '')}, Frequency Error: {call_data.get('freq_error', '')}, Signal: {call_data.get('signal', '')}, Noise: {call_data.get('noise', '')}, Call Length: {call_data.get('call_length', '')} seconds",
        "-movflags", "frag_keyframe+empty_moov",
        m4a_file_path
    ]

    if compression_config.get('use_loudnorm', False):
        command.insert(4, "-af")
        command.insert(5, "apad=whole_dur=3s,loudnorm=I=-16:TP=-1.5:LRA=11")

    try:
        # Execute the ffmpeg command
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        module_logger.debug(f"ffmpeg output: {result.stdout}")
        module_logger.info(f"Successfully converted WAV to M4A for file: {wav_file_path}")
        return True
    except subprocess.CalledProcessError as e:
        error_message = f"Failed to convert WAV to M4A for file {wav_file_path}. Error: {e}"
        module_logger.error(error_message)
        return False
    except Exception as e:
        error_message = f"An unexpected error occurred during conversion of {wav_file_path}: {e}"
        module_logger.error(error_message)
        return False


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
