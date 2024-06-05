import json
import logging
import traceback
from datetime import datetime, timezone

import requests
from requests import HTTPError, Timeout, RequestException

from lib.helper_handler import generate_mapped_json

module_logger = logging.getLogger("icad_tr_consumer.webhook_handler")


class WebHook:
    def __init__(self, webhook_config_data, call_data):
        self.webhook_config_data = webhook_config_data
        self.call_data = call_data

    def _send_request(self, url, webhook_headers, webhook_body):
        try:
            if webhook_headers is None:
                webhook_headers = {'Content-Type': 'application/json'}
            elif 'Content-Type' not in webhook_headers:
                webhook_headers['Content-Type'] = 'application/json'

            if webhook_body is None:
                module_logger.error(F'No Webhook Body Can not post webhook.')
                return

            response = requests.post(url,
                                     json=webhook_body,
                                     headers=webhook_headers,
                                     timeout=10
                                     )

            response.raise_for_status()  # Raise an exception for HTTP errors

            module_logger.debug(f"<<Webhook>> Successful: URL {url}")
            return True
        except HTTPError as e:
            module_logger.error(
                f"<<Webhook>> <<Failed>> HTTP error occurred for URL {url}: {e.response.status_code} {e.response.text}")
            return False
        except ConnectionError as e:
            module_logger.error(f"<<Webhook>> <<Failed>> Connection error occurred for URL {url}: {e}")
            return False
        except Timeout as e:
            module_logger.error(f"<<Webhook>> <<Failed>> Timeout error occurred for URL {url}: {e}")
            return False
        except RequestException as e:
            module_logger.error(f"<<Webhook>> <<Failed>> Request error occurred for URL {url}: {e}")
            return False
        except Exception as e:
            traceback.print_exc()
            module_logger.error(f"<<Webhook>> <<Failed>> Unexpected error occurred for URL {url}: {e}")
            return False

    def send_webhook(self):

        try:

            webhook_header_json = generate_mapped_json(self.webhook_config_data.get("webhook_headers", {}), self.call_data)
            webhook_body_json = generate_mapped_json(self.webhook_config_data.get("webhook_body", {}), self.call_data)
            webhook_url = self.webhook_config_data.get("webhook_url", "")


            # Log the webhook JSON to debug issues with the payload
            module_logger.debug(f"Webhook URL: {webhook_url}")
            module_logger.debug(f"Webhook Header JSON: {webhook_header_json}")
            module_logger.debug(f"Webhook Body JSON: {webhook_body_json}")

            post_result = self._send_request(webhook_url, webhook_header_json, webhook_body_json)
            return post_result
        except Exception as e:
            traceback.print_exc()
            module_logger.error(f"<<Webhook>> Post Failure:\n {repr(e)}")
            return False
