import logging
import os
from datetime import datetime

from lib.remote_storage_handler import get_archive_class

module_logger = logging.getLogger('icad_tr_consumer.archive')


def archive_audio_files(archive_config, source_path, call_data):
    wav_url_path = None
    m4a_url_path = None
    json_url_path = None

    if not archive_config.get("archive_path", "") and archive_config.get('archive_type', '') not in ["google_cloud",
                                                                                                     "aws_s3"]:
        module_logger.warning("Cannot Archive Files. No Archive Path Set")
        return wav_url_path, m4a_url_path, json_url_path

    if not archive_config.get('archive_type', '') or archive_config.get('archive_type', '') not in ["google_cloud",
                                                                                                    "aws_s3", "scp",
                                                                                                    "local"]:
        module_logger.warning(
            f"Cannot Archive Files. Archive Type Not Set or Invalid. {archive_config.get('archive_type', '')}")
        return wav_url_path, m4a_url_path, json_url_path

    archive_class = get_archive_class(archive_config)
    if not archive_class:
        module_logger.warning(f"Can not start the Archive Class for {archive_config.get('archive_type', '')}")
        return wav_url_path, m4a_url_path, json_url_path

    # Convert the epoch timestamp to a datetime object in UTC
    call_date = datetime.utcfromtimestamp(call_data['start_time'])

    system_short_name = call_data.get("short_name", "unknown")

    generated_folder_path = os.path.join(system_short_name, str(call_date.year),
                                         str(call_date.month), str(call_date.day))

    # Create folder structure using current date
    folder_path = os.path.join(archive_config.get("archive_path"), generated_folder_path)

    wav_filename = call_data.get("filename")
    m4a_filename = wav_filename.replace(".wav", ".m4a")
    json_filename = wav_filename.replace(".wav", ".json")

    source_wav_path = os.path.join(source_path, wav_filename)
    destination_wav_path = os.path.join(folder_path, wav_filename)

    source_m4a_path = os.path.join(source_path, m4a_filename)
    destination_m4a_path = os.path.join(folder_path, m4a_filename)

    source_json_path = os.path.join(source_path, json_filename)
    destination_json_path = os.path.join(folder_path, json_filename)

    module_logger.info(
        f"Archiving {' '.join(archive_config.get('archive_extensions', []))} files via {archive_config.get('archive_type', '')} to: {folder_path}")

    for extension in archive_config.get('archive_extensions', []):
        if extension == ".wav":
            if os.path.isfile(source_wav_path):
                upload_response = archive_class.upload_file(source_wav_path, destination_wav_path,
                                                            generated_folder_path)
                if upload_response:
                    wav_url_path = upload_response
            else:
                module_logger.warning("Skipping archive for WAV file. File does not exist.")
        elif extension == ".m4a":
            if os.path.isfile(source_m4a_path):
                upload_response = archive_class.upload_file(source_m4a_path, destination_m4a_path,
                                                            generated_folder_path)
                if upload_response:
                    m4a_url_path = upload_response
            else:
                module_logger.warning("Skipping archive for M4A file. File does not exist.")
        elif extension == ".json":
            if os.path.isfile(source_json_path):
                upload_response = archive_class.upload_file(source_json_path, destination_json_path,
                                                            generated_folder_path)
                if upload_response:
                    json_url_path = upload_response
            else:
                module_logger.warning("Skipping archive for JSON file. File does not exist.")
        else:
            module_logger.warning("Unknown Archive Extension")

    if archive_config.get("archive_days", 0) >= 1:
        archive_class.clean_files(os.path.join(archive_config.get("archive_path"), system_short_name),
                                  archive_config.get("archive_days", 1))

    return wav_url_path, m4a_url_path, json_url_path
