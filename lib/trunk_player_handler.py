import os
from datetime import datetime

import requests
import logging

module_logger = logging.getLogger('icad_tr_consumer.trunk_player')


def upload_to_trunk_player(player_config, call_data):
    url = player_config['api_url']
    module_logger.info(f'Uploading To Trunk Player: {url}')

    call_date = datetime.utcfromtimestamp(call_data['start_time'])

    system_short_name = call_data.get("short_name", "unknown")

    generated_folder_path = os.path.join(system_short_name, str(call_date.year),
                                         str(call_date.month), str(call_date.day))

    try:
        trunk_player_json = {"auth_token": player_config.get("api_key"), "file_path": f"{generated_folder_path}/", "file_name": call_data.get("filename").split(".wav")[-1], "m4a": True}
        response = requests.post(url, json=trunk_player_json)

        response.raise_for_status()
        module_logger.info(
            f"Successfully uploaded to Trunk Player: {url}")
        return True
    except requests.exceptions.RequestException as e:
        # This captures HTTP errors, connection errors, etc.
        module_logger.error(f'Failed Uploading To Trunk Player: {e}')
    except Exception as e:
        # Catch-all for any other unexpected errors
        module_logger.error(f'An unexpected error occurred while upload to Trunk Player {url}: {e}')

    return False
