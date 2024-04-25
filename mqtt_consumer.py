import os
import time
import traceback

from lib.config_handler import load_config_file
from lib.logging_handler import CustomLogger
from lib.mqtt_handler import MQTTClient

app_name = "icad_tr_consumer"
__version__ = "1.0"

root_path = os.getcwd()
config_file_name = "config.json"

log_file_name = f"{app_name}.log"

log_path = os.path.join(root_path, 'log')

if not os.path.exists(log_path):
    os.makedirs(log_path)

config_path = os.path.join(root_path, 'etc')

logging_instance = CustomLogger(1, f'{app_name}',
                                os.path.join(log_path, log_file_name))

try:
    config_data = load_config_file(os.path.join(config_path, config_file_name))
    logging_instance.set_log_level(config_data["log_level"])
    logger = logging_instance.logger
    logger.info("Loaded Config File")
except Exception as e:
    traceback.print_exc()
    print(f'Error while <<loading>> configuration : {e}')
    time.sleep(5)
    exit(1)


def main():
    logger.debug("Running Main")

    mqtt_client = MQTTClient(config_data).start_mqtt_connection()

    try:
        while True:
            time.sleep(1)  # Sleep to allow signal handling or simple background processing
    except KeyboardInterrupt as e:
        logger.info(f"Shutting Down MQTT Consumer: {e}")
    except Exception as e:
        traceback.print_exc()
        logger.info(f"Unknown Exception occurred while running consumer. {e}")
    finally:
        if mqtt_client:
            mqtt_client.disconnect()


if __name__ == '__main__':
    main()
