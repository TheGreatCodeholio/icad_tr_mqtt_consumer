import json
import os
import time
from datetime import datetime, timezone

import requests
import logging

module_logger = logging.getLogger('icad_tr_consumer.icad_dispatch_uploader')


def upload_to_icad_dispatch(icad_dispatch_data, temp_path, call_data):
    """
    Uploads call data to iCAD Dispatch,
    """

    module_logger.info(f'Attempting upload to RDIO: {icad_dispatch_data["url"]}')

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
        "key":           (None, icad_dispatch_data['api_key']),
        "audioName":     (None, wav_filename),
        "audioType":     (None, "audio/x-wav"),
        "dateTime":      (None, formatted_time),
        "frequencies":   (None, json.dumps(call_data.get('freqList', []))),
        "frequency":     (None, str(call_data.get('freq', ''))),
        "patches":       (None, json.dumps(call_data.get('patches', []))),
        "sources":       (None, json.dumps(call_data.get('srcList', []))),
        "system":        (None, str(icad_dispatch_data['system_id'])),
        "systemLabel":   (None, call_data.get('short_name', '')),
        "talkgroup":     (None, str(call_data.get('talkgroup', ''))),
        "talkgroupGroup":(None, call_data.get('talkgroup_group', '')),
        "talkgroupLabel":(None, call_data.get('talkgroup_description', '')),
        "talkgroupTag":  (None, call_data.get('talkgroup_tag', '')),
    }


    file_obj = open(wav_path, 'rb')
    multipart_fields["audio"] = (wav_filename, file_obj, "audio/x-wav")

    module_logger.debug(f"Sending to {icad_dispatch_data['url']} with fields:")
    for field_name, field_value in multipart_fields.items():
        module_logger.debug(f"  {field_name} => {field_value[1]}")

    try:
        # This sends a multipart/form-data request with text fields + optional file
        response = requests.post(icad_dispatch_data['url'], files=multipart_fields)
        response.raise_for_status()

        module_logger.info(f"Successfully uploaded to iCAD Dispatch: HTTP {response.status_code} - {response.text}")
        return True

    except requests.exceptions.SSLError as ssl_err:
        module_logger.error(
            f"SSL error while uploading to iCAD Dispatch {icad_dispatch_data['url']}: {ssl_err}",
            exc_info=True
        )
    except requests.exceptions.HTTPError as http_err:
        status_code = http_err.response.status_code if http_err.response else 'Unknown'
        response_text = http_err.response.text if http_err.response else 'No response text'
        module_logger.error(
            f"HTTPError while uploading to iCAD Dispatch {icad_dispatch_data['url']}: "
            f"Status {status_code}, Response: {response_text}",
            exc_info=True
        )
    except requests.exceptions.ConnectionError as conn_err:
        module_logger.error(
            f"ConnectionError while uploading to iCAD Dispatch {icad_dispatch_data['url']}: {conn_err}",
            exc_info=True
        )
    except requests.exceptions.Timeout as timeout_err:
        module_logger.error(
            f"Timeout while uploading to iCAD Dispatch {icad_dispatch_data['url']}: {timeout_err}",
            exc_info=True
        )
    except requests.exceptions.RequestException as req_err:
        module_logger.error(
            f"RequestException while uploading to iCAD Dispatch {icad_dispatch_data['url']}: {req_err}",
            exc_info=True
        )
    except Exception as e:
        module_logger.error(
            f"Unexpected error during iCAD Dispatch upload {icad_dispatch_data['url']}: {e}",
            exc_info=True
        )
    finally:
        # Make sure to close the file if we opened it
        if file_obj and not file_obj.closed:
            file_obj.close()

    return False
