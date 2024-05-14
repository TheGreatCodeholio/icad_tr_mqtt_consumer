import json
import logging
import os
import subprocess

module_logger = logging.getLogger('icad_tr_consumer.audio_file_handler')


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

def compress_wav(compression_config, wav_file_path):
    # Check if the WAV file exists
    if not os.path.isfile(wav_file_path):
        module_logger.error(f"WAV file does not exist: {wav_file_path}")
        return False

    module_logger.info(
        f'Converting WAV to M4A at {compression_config.get("sample_rate")}@{compression_config.get("bitrate", 96)}')

    # Construct the ffmpeg command
    m4a_file_path = wav_file_path.replace('.wav', '.m4a')
    command = ["ffmpeg", "-y", "-i", wav_file_path, "-af", "aresample=resampler=soxr", "-ar",
               f"{compression_config.get('sample_rate', 16000)}", "-c:a", "aac",
               "-ac", "1", "-b:a", f"{compression_config.get('bitrate', 96)}k", m4a_file_path]

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
