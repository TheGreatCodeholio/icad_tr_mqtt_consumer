import json
import os

import requests
import logging

module_logger = logging.getLogger('icad_tr_consumer.icad_cloud_detect')


def upload_to_icad_cloud_detect(cloud_detect_config, temp_path, call_data):
    url = cloud_detect_config.get('api_url', "")
    api_key = cloud_detect_config.get("api_key", "")
    module_logger.info(f'Uploading To iCAD Cloud Detect: {url}')

    # Build Headers with API Auth Key
    request_header = {
        "Authorization": api_key
    }


    try:

        json_string = json.dumps(call_data)
        json_bytes = json_string.encode('utf-8')



        wav_path = os.path.join(temp_path, f'{call_data.get("filename")}')

        with open(wav_path, 'rb') as audio_file:
            files = {
                'audioFile': (call_data['filename'], audio_file, 'audio/x-wav'),
                'jsonFile': (call_data['filename'].replace('.wav', '.json'), json_bytes, 'application/json')
            }

            response = requests.post(url, headers=request_header, files=files)

            response.raise_for_status()
            module_logger.info(f"Successfully uploaded to iCAD Cloud Detect: {url}")
            return True
    except requests.exceptions.RequestException as e:
        if e.response is not None:
            try:
                error_message = e.response.json().get("message", "No detailed message provided")
            except ValueError:
                error_message = "No detailed message provided"
            module_logger.error(f'Failed Uploading To iCAD Cloud Detect: {e.response.status_code} - {error_message}')
        else:
            module_logger.error(f'Failed Uploading To iCAD Cloud Detect: {str(e)}')
    except Exception as e:
        # Catch-all for any other unexpected errors
        module_logger.error(f'An unexpected error occurred while upload to iCAD Cloud Detect {url}: {e}')

    return False