import requests
import logging

module_logger = logging.getLogger('icad_tr_consumer.icad_alerting')


def upload_to_icad_alert(alert_config, call_data):
    url = alert_config.get('api_url', "")
    api_key = alert_config.get("api_key", "")
    module_logger.info(f'Uploading To iCAD Alerting: {url}')

    # Build Headers with API Auth Key
    api_headers = {
        "Authorization": api_key
    }

    try:
        response = requests.post(url, headers=api_headers, json=call_data, verify=alert_config.get('verify_ssl', False))

        response.raise_for_status()
        module_logger.info(
            f"Successfully uploaded to iCAD Alerting: {url}")
        return True
    except requests.exceptions.RequestException as e:
        if e.response is not None:
            try:
                error_message = e.response.json().get("message", "No detailed message provided")
            except ValueError:
                error_message = "No detailed message provided"
            module_logger.error(f'Failed Uploading To iCAD Alerting: {e.response.status_code} - {error_message}')
        else:
            module_logger.error(f'Failed Uploading To iCAD Alerting: {str(e)}')
    except Exception as e:
        # Catch-all for any other unexpected errors
        module_logger.error(f'An unexpected error occurred while upload to iCAD Alerting {url}: {e}')

    return False