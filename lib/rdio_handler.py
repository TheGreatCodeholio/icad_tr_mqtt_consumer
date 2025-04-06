import json
import os
import time
from datetime import datetime, timezone

import requests
import logging

module_logger = logging.getLogger('icad_tr_consumer.rdio_uploader')


def upload_to_rdio(rdio_data, temp_path, call_data):
    """
    Uploads call data to RDIO, optionally including the WAV file if the URL is not localhost.

    If "127.0.0.1" in rdio_data['rdio_url'], we skip attaching the audio file,
    but still send everything as multipart/form-data.
    """

    module_logger.info(f'Attempting upload to RDIO: {rdio_data["rdio_url"]}')

    wav_filename = call_data.get("filename")
    if not wav_filename:
        module_logger.error("No 'filename' found in call_data. Aborting.")
        return False

    # Full path to the WAV file
    wav_path = os.path.join(temp_path, wav_filename)
    if not os.path.isfile(wav_path):
        module_logger.error(f"WAV file not found at '{wav_path}'. Aborting upload.")
        return False

    # Convert epoch to UTC time string
    timestamp = call_data.get('start_time', time.time())
    utc_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    formatted_time = utc_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    multipart_fields = {
        "key":           (None, rdio_data['rdio_api_key']),
        "audioName":     (None, wav_filename),
        "audioType":     (None, "audio/x-wav"),
        "audioUrl":      (None, call_data.get("audio_m4a_url", "")),
        "dateTime":      (None, formatted_time),
        "frequencies":   (None, json.dumps(call_data.get('freqList', []))),
        "frequency":     (None, str(call_data.get('freq', ''))),
        "patches":       (None, json.dumps(call_data.get('patches', []))),
        "sources":       (None, json.dumps(call_data.get('srcList', []))),
        "system":        (None, str(rdio_data['system_id'])),
        "systemLabel":   (None, call_data.get('short_name', '')),
        "talkgroup":     (None, str(call_data.get('talkgroup', ''))),
        "talkgroupGroup":(None, call_data.get('talkgroup_group', '')),
        "talkgroupLabel":(None, call_data.get('talkgroup_description', '')),
        "talkgroupTag":  (None, call_data.get('talkgroup_tag', '')),
    }

    # If the URL does NOT contain 127.0.0.1, attach the actual WAV file.
    # Otherwise, skip it (but still do multipart for the other fields).
    use_remote_storage = rdio_data.get("remote_storage", False)
    if not use_remote_storage:
        # Add an actual file part
        file_obj = open(wav_path, 'rb')
        multipart_fields["audio"] = (wav_filename, file_obj, "audio/x-wav")
    else:
        if not call_data.get("audio_m4a_url", ""):
            module_logger.error("M4A URL doesn't exist, we can't use remote storage for RDIO. Falling back to file upload.")
            file_obj = open(wav_path, 'rb')
            multipart_fields["audio"] = (wav_filename, file_obj, "audio/x-wav")
        else:
            file_obj = None

    use_remote_storage = rdio_data.get("remote_storage", False)
    audio_m4a_url = call_data.get("audio_m4a_url", "")

    if use_remote_storage and audio_m4a_url:
        module_logger.debug("Using remote storage with valid M4A URL.")
    else:
        # Fallback: either remote storage is disabled OR no URL provided
        if use_remote_storage and not audio_m4a_url:
            module_logger.error(
            "M4A URL doesn't exist; we can't use remote storage for RDIO. Falling back to file upload."
            )
        file_obj = open(wav_path, 'rb')
        multipart_fields["audio"] = (wav_filename, file_obj, "audio/x-wav")

    module_logger.debug(f"Sending to {rdio_data['rdio_url']} with fields:")
    for field_name, field_value in multipart_fields.items():
        module_logger.debug(f"  {field_name} => {field_value[1]}")

    try:
        # This sends a multipart/form-data request with text fields + optional file
        response = requests.post(rdio_data['rdio_url'], files=multipart_fields)
        response.raise_for_status()

        module_logger.info(f"Successfully uploaded to RDIO: HTTP {response.status_code} - {response.text}")
        return True

    except requests.exceptions.SSLError as ssl_err:
        module_logger.error(
            f"SSL error while uploading to RDIO {rdio_data['rdio_url']}: {ssl_err}",
            exc_info=True
        )
    except requests.exceptions.HTTPError as http_err:
        status_code = http_err.response.status_code if http_err.response else 'Unknown'
        response_text = http_err.response.text if http_err.response else 'No response text'
        module_logger.error(
            f"HTTPError while uploading to RDIO {rdio_data['rdio_url']}: "
            f"Status {status_code}, Response: {response_text}",
            exc_info=True
        )
    except requests.exceptions.ConnectionError as conn_err:
        module_logger.error(
            f"ConnectionError while uploading to RDIO {rdio_data['rdio_url']}: {conn_err}",
            exc_info=True
        )
    except requests.exceptions.Timeout as timeout_err:
        module_logger.error(
            f"Timeout while uploading to RDIO {rdio_data['rdio_url']}: {timeout_err}",
            exc_info=True
        )
    except requests.exceptions.RequestException as req_err:
        module_logger.error(
            f"RequestException while uploading to RDIO {rdio_data['rdio_url']}: {req_err}",
            exc_info=True
        )
    except Exception as e:
        module_logger.error(
            f"Unexpected error during RDIO upload {rdio_data['rdio_url']}: {e}",
            exc_info=True
        )
    finally:
        # Make sure to close the file if we opened it
        if file_obj and not file_obj.closed:
            file_obj.close()

    return False