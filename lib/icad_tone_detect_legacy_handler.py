import json
import os

import requests
import logging

module_logger = logging.getLogger('icad_tr_consumer.icad_uploader')


def upload_to_icad_legacy(icad_data, temp_path, call_data):
    module_logger.info(f'Uploading to iCAD Tone Detect API: {icad_data["icad_url"]}')

    wav_path = os.path.join(temp_path, f'{call_data.get("filename")}')

    if not call_data:
        module_logger.error('Failed uploading to iCAD API: Empty call_data JSON')
        return False

    try:
        with open(wav_path, 'rb') as audio_file:
            files = {'file': (wav_path, audio_file, 'audio/x-wav')}
            response = requests.post(icad_data['icad_url'], files=files, data=call_data)
            response.raise_for_status()  # This will raise an error for 4xx and 5xx responses
            module_logger.info(f'Successfully uploaded to iCAD API: {response.status_code}, {response.text}')
            return True

    except FileNotFoundError:
        module_logger.error(f'File not found: {wav_path}')
    except requests.exceptions.HTTPError as e:
        # This captures HTTP errors and logs them. `e.response` contains the response that caused this error.
        module_logger.error(f'HTTP error uploading to iCAD API: {e.response.status_code}, {e.response.text}')
    except requests.exceptions.RequestException as e:
        # This captures other requests-related errors
        module_logger.error(f'Error uploading to iCAD API: {e}')
    except IOError as e:
        # This captures general IO errors (broader than just FileNotFoundError)
        module_logger.error(f'IO error with file: {wav_path}, {e}')

    return False
