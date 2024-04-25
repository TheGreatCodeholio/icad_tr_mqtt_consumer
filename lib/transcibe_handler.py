import io
import json

import requests
import logging

module_logger = logging.getLogger('icad_tr_consumer.transcribe')


def upload_to_transcribe(transcribe_config, wav_data, call_data, talkgroup_config=None):
    url = transcribe_config['api_url']
    module_logger.info(f'Uploading To Transcribe API: {url}')

    config_data = {}  # Initialize as an empty dict

    if talkgroup_config:
        config_data['whisper_config_data'] = json.dumps(talkgroup_config.get("whisper", {}))

    try:
        json_string = json.dumps(call_data)
        json_bytes = json_string.encode('utf-8')

        audio_file_like = io.BytesIO(wav_data)

        audio_file_like.seek(0)

        data = {
            'audioFile': audio_file_like,
            'jsonFile': json_bytes
        }

        response = requests.post(url, files=data, data=config_data)
        response.raise_for_status()  # This will raise an error for 4xx and 5xx responses
        response_json = response.json()
        module_logger.info(f'Successfully received transcript data from: {url}')

        audio_file_like.close()

        return response_json

    except requests.exceptions.HTTPError as err:
        module_logger.error(f"Transcribe - HTTP error occurred: {err}")
        return None
    except Exception as err:
        module_logger.error(f"Error uploading to Transcribe API: {err}")
        return None
