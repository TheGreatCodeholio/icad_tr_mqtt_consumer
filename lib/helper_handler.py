import json
from datetime import datetime, timezone


def generate_mapped_json(template, data):
    """
    Generates JSON content by replacing placeholders with actual data.

    :param template: Template JSON string or dictionary with placeholders
    :param data: Dictionary containing data to replace placeholders
    :return: Mapped JSON string or dictionary
    """
    try:
        # Convert the epoch timestamp to a datetime object and format it
        if 'start_time' in data:
            current_time_dt = datetime.fromtimestamp(data['start_time'], tz=timezone.utc).astimezone()
            current_time = current_time_dt.strftime('%H:%M %b %d %Y %Z')
            data['timestamp'] = current_time
            data['timestamp_epoch'] = data['start_time']

        # Function to get value from nested dictionary using dot notation
        def get_value_from_dict(d, key):
            keys = key.split('.')
            for k in keys:
                d = d.get(k, '')
                if d == '':
                    break
            return d

        # Recursive function to replace placeholders in the JSON object
        def replace_placeholders(obj, mapping):
            if isinstance(obj, dict):
                return {replace_placeholders(k, mapping): replace_placeholders(v, mapping) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [replace_placeholders(i, mapping) for i in obj]
            elif isinstance(obj, str):
                while True:
                    start_idx = obj.find('{')
                    end_idx = obj.find('}', start_idx)
                    if start_idx == -1 or end_idx == -1:
                        break
                    placeholder = obj[start_idx + 1:end_idx]
                    value = get_value_from_dict(mapping, placeholder)
                    obj = obj[:start_idx] + str(value) + obj[end_idx + 1:]
                return obj
            else:
                return obj

        # Check if the template is a string (JSON) or a dictionary
        if isinstance(template, str):
            json_data = json.loads(template)
        elif isinstance(template, dict):
            json_data = template
        else:
            raise ValueError("Template must be a JSON string or a dictionary")

        mapped_json_data = replace_placeholders(json_data, data)

        # Convert back to JSON string if the input was a string
        if isinstance(template, str):
            return json.dumps(mapped_json_data, indent=2)
        else:
            return mapped_json_data

    except Exception as e:
        print(f"Failed to generate mapped JSON content: {e}")
        return None
