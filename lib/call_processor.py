import logging
import os

from lib.archive_handler import archive_files
from lib.audio_file_handler import save_temporary_files, compress_wav, save_temporary_json_file, clean_temp_files
from lib.broadcastify_calls_handler import upload_to_broadcastify_calls
from lib.config_handler import get_talkgroup_config
from lib.icad_alerting_handler import upload_to_icad_alert
from lib.icad_player_handler import upload_to_icad_player
from lib.icad_tone_detect_legacy_handler import upload_to_icad_legacy
from lib.openmhz_handler import upload_to_openmhz
from lib.rdio_handler import upload_to_rdio
from lib.tone_detect_handler import get_tones
from lib.transcibe_handler import upload_to_transcribe
from lib.webhook_handler import WebHook

module_logger = logging.getLogger('icad_tr_consumer.call_processing')


def process_mqtt_call(global_config_data, wav_data, call_data):
    m4a_exists = False
    short_name = call_data.get("short_name", "")
    talkgroup_decimal = call_data.get("talkgroup", 0)

    if not short_name:
        module_logger.warning("System Short Name not in call metadata. Cannot Process")
        return

    system_config = global_config_data.get("systems", {}).get(short_name, {})
    if not system_config:
        module_logger.warning("System configuration not in config data. Cannot Process")
        return

    talkgroup_config = get_talkgroup_config(system_config.get("talkgroup_config", {}), call_data)
    if not talkgroup_config:
        module_logger.warning("Talkgroup configuration not in config data. Cannot Process")
        return

    call_data["tones"] = {}
    call_data["transcript"] = []

    # save files temporarily in /dev/shm
    temp_result = save_temporary_files(global_config_data.get("temp_file_path", "/dev/shm"), wav_data, call_data)
    if not temp_result:
        return

    # Convert WAV to M4A in tmp /dev/shm
    if system_config.get("audio_compression", {}).get("enabled", 0) == 1:
        m4a_exists = compress_wav(system_config.get("audio_compression", {}),
                                  os.path.join(global_config_data.get("temp_file_path", "/dev/shm"),
                                               call_data.get("filename")))

    # Legacy Tone Detection
    for icad_detect in system_config.get("icad_tone_detect_legacy", []):
        if icad_detect.get("enabled", 0) == 1:
            try:
                icad_result = upload_to_icad_legacy(icad_detect, global_config_data.get("temp_file_path", "/dev/shm"),
                                                    call_data)
                if icad_result:
                    module_logger.info(f"Successfully uploaded to iCAD Detect server: {icad_detect.get('icad_url')}")
                else:
                    raise Exception()


            except Exception as e:
                module_logger.error(
                    f"Failed to upload to iCAD Detect server: {icad_detect.get('icad_url')}. Error: {str(e)}",
                    exc_info=True)
                continue
        else:
            module_logger.warning(f"iCAD Detect is disabled: {icad_detect.get('icad_url')}")
            continue

    # Tone Detection
    if system_config.get("tone_detection", {}).get("enabled", 0) == 1:
        if talkgroup_decimal not in system_config.get("tone_detection", {}).get("allowed_talkgroups",
                                                                                []) and "*" not in system_config.get(
            "tone_detection", {}).get("allowed_talkgroups", []):
            module_logger.debug(
                f"Tone Detection Disabled for Talkgroup {call_data.get('talkgroup_tag') or call_data.get('talkgroup')}")
        else:
            tone_detect_result = get_tones(system_config.get("tone_detection", {}), wav_data)
            call_data["tones"] = tone_detect_result
            module_logger.info(f"Tone Detection Complete")
            module_logger.debug(call_data.get("tones"))

    # Transcribe Audio
    if system_config.get("transcribe", {}).get("enabled", 0) == 1:
        if talkgroup_decimal not in system_config.get("transcribe", {}).get("allowed_talkgroups",
                                                                            []) and "*" not in system_config.get(
            "transcribe", {}).get("allowed_talkgroups", []):
            module_logger.debug(
                f"Transcribe Disabled for Talkgroup {call_data.get('talkgroup_tag') or call_data.get('talkgroup')}")
        else:
            transcribe_result = upload_to_transcribe(system_config.get("transcribe", {}), wav_data, call_data,
                                                     talkgroup_config=None)
            call_data["transcript"] = transcribe_result
            module_logger.info(f"Audio Transcribe Complete")
            module_logger.debug(call_data.get("transcript"))

    # Resave JSON with new Transcript and Tone Data.
    try:
        save_temporary_json_file(global_config_data.get("temp_file_path", "/dev/shm"), call_data)
    except Exception as e:
        module_logger.warning(f"Enable to save new call data to temporary file. {e}")

    # Archive Audio Files
    if system_config.get("archive", {}).get("enabled", 0) == 1:
        wav_url, m4a_url, json_url = archive_files(system_config.get("archive", {}),
                                                   global_config_data.get("temp_file_path", "/dev/shm"), call_data)
        if wav_url:
            call_data["audio_wav_url"] = wav_url
        if m4a_url:
            call_data["audio_m4a_url"] = m4a_url

        if wav_url is None and m4a_url is None and json_url is None:
            module_logger.error("No Files Uploaded to Archive")
        else:
            module_logger.info(f"Archive Complete")
            module_logger.debug(f"Url Paths:\n{call_data.get('audio_wav_url')}\n{call_data.get('audio_m4a_url')}")

    # Send to Players

    # Upload to OpenMHZ
    if system_config.get("openmhz", {}).get("enabled", 0) == 1:
        if m4a_exists:
            openmhz_result = upload_to_openmhz(system_config.get("openmhz", {}),
                                               global_config_data.get("temp_file_path", "/dev/shm"), call_data)
        else:
            module_logger.warning(f"No M4A file can't send to OpenMHZ")

    # Upload to BCFY Calls
    if system_config.get("broadcastify_calls", {}).get("enabled", 0) == 1:
        if m4a_exists:
            bcfy_calls_result = upload_to_broadcastify_calls(system_config.get("broadcastify_calls", {}),
                                                             global_config_data.get("temp_file_path", "/dev/shm"),
                                                             call_data)
        else:
            module_logger.warning(f"No M4A file can't send to Broadcastify Calls")

    # Upload to iCAD Player
    if call_data.get("audio_m4a_url", "") and system_config.get("icad_player", {}).get("enabled", 0) == 1:
        if talkgroup_decimal not in system_config.get("icad_player", {}).get("allowed_talkgroups",
                                                                             []) and "*" not in system_config.get(
            "icad_player", {}).get("allowed_talkgroups", []):
            module_logger.warning(
                f"iCAD Player Disabled for Talkgroup {call_data.get('talkgroup_tag') or call_data.get('talkgroup_decimal')}")
        else:
            upload_to_icad_player(system_config.get("icad_player", {}), call_data)
            module_logger.info(f"Upload to iCAD Player Complete")

    # Upload to RDIO systems
    for rdio in system_config.get("rdio_systems", []):
        if rdio.get("enabled", 0) == 1:
            if not m4a_exists:
                module_logger.warning(f"No M4A file can't send to RDIO")
                continue
            try:
                upload_to_rdio(rdio, global_config_data.get("temp_file_path", "/dev/shm"), call_data)
                module_logger.info(f"Successfully uploaded to RDIO server: {rdio.get('rdio_url')}")
            except Exception as e:
                module_logger.error(f"Failed to upload to RDIO server: {rdio.get('rdio_url')}. Error: {str(e)}",
                                    exc_info=True)
                continue
        else:
            module_logger.warning(f"RDIO system is disabled: {rdio.get('rdio_url')}")
            continue

    # Upload to Alerting
    if system_config.get("icad_alerting", {}).get("enabled", 0) == 1:
        if talkgroup_decimal not in system_config.get("icad_alerting", {}).get("allowed_talkgroups",
                                                                               []) and "*" not in system_config.get(
            "icad_alerting", {}).get("allowed_talkgroups", []):
            module_logger.warning(
                f"iCAD Alerting Disabled for Talkgroup {call_data.get('talkgroup_tag') or call_data.get('talkgroup_decimal')}")
        else:
            upload_to_icad_alert(system_config.get("icad_alerting", {}), call_data)
            module_logger.info(f"Upload to iCAD Alert Complete")

    # Send To Webhooks
    for webhook in system_config.get("webhooks", []):
        if webhook.get("enabled", 0) == 1:
            module_logger.info(f"Processing Webhook {webhook.get('webhook_url')}")
            if talkgroup_decimal not in webhook.get("allowed_talkgroups", []) and "*" not in webhook.get("allowed_talkgroups", []):
                module_logger.warning(
                    f"Webhook Disabled for Talkgroup {call_data.get('talkgroup_tag') or call_data.get('talkgroup_decimal')}")
            else:
                wh = WebHook(webhook, call_data)
                wh.send_webhook()
        else:
            module_logger.debug(f"Webhook is disabled: {webhook.get('webhook_url')}")

    # Cleanup Temp Files
    clean_temp_files(global_config_data.get("temp_file_path", "/dev/shm"), call_data)
