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

        # Special handling for constructing transcript from segments
        def construct_transcript_from_segments(segments):
            return "\n".join(segment['text'] for segment in segments)

        # Special handling for joining addresses list
        def join_addresses(addresses):
            return ", ".join(addresses)

        # Special handling for generating tone reports
        def generate_tone_report(tones, report_type):
            if report_type == 'text':
                report_lines = []
                if 'two_tone' in tones:
                    for tone in tones['two_tone']:
                        report_lines.append(f"Two-tone ID: {tone['tone_id']} - A: {tone['detected'][0]} Hz, B: {tone['detected'][1]} Hz, Duration: {tone['tone_a_length']}s, {tone['tone_b_length']}s")
                if 'long_tone' in tones:
                    for tone in tones['long_tone']:
                        report_lines.append(f"Long-tone ID: {tone['tone_id']} - Frequency: {tone['detected']} Hz, Duration: {tone['length']}s")
                if 'hi_low_tone' in tones:
                    for tone in tones['hi_low_tone']:
                        report_lines.append(f"Hi-Low tone ID: {tone['tone_id']} - Frequencies: {tone['detected'][0]} Hz, {tone['detected'][1]} Hz, Duration: {tone['length']}s")
                return "\n".join(report_lines)
            elif report_type == 'html':
                report_lines = []
                if 'two_tone' in tones:
                    for tone in tones['two_tone']:
                        report_lines.append(f"<p>Two-tone ID: {tone['tone_id']} - A: {tone['detected'][0]} Hz, B: {tone['detected'][1]} Hz, Duration: {tone['tone_a_length']}s, {tone['tone_b_length']}s</p>")
                if 'long_tone' in tones:
                    for tone in tones['long_tone']:
                        report_lines.append(f"<p>Long-tone ID: {tone['tone_id']} - Frequency: {tone['detected']} Hz, Duration: {tone['length']}s</p>")
                if 'hi_low_tone' in tones:
                    for tone in tones['hi_low_tone']:
                        report_lines.append(f"<p>Hi-Low tone ID: {tone['tone_id']} - Frequencies: {tone['detected'][0]} Hz, {tone['detected'][1]} Hz, Duration: {tone['length']}s</p>")
                return "".join(report_lines)

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
                    if placeholder == 'transcript.segments_text':
                        value = construct_transcript_from_segments(mapping.get('transcript', {}).get('segments', []))
                    elif placeholder == 'transcript.addresses_text':
                        value = join_addresses(mapping.get('transcript', {}).get('addresses', []))
                    elif placeholder == 'tones.report_html':
                        value = generate_tone_report(data['tones'], 'html')
                    elif placeholder == 'tones.report_text':
                        value = generate_tone_report(data['tones'], 'text')
                    else:
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
