import io
import traceback
from typing import Dict, Any

# If you want granular logging per failure type and they are exposed:
# from icad_tone_detection.exceptions import (
#     AudioLoadError, FrequencyExtractionError, ToneDetectionError, FFmpegNotFoundError
# )
from icad_tone_detection import tone_detect  # adjust import if you namespaced differently


def get_tones(tone_detect_config: Dict[str, Any], wav_data: bytes, module_logger=None) -> Dict[str, list]:
    """
    Run tone detection using the updated icad_tone_detection.tone_detect() API.

    Returns dict with keys: two_tone, long_tone, hi_low_tone, mdc, dtmf
    """
    detected_tones = {
        "two_tone": [],
        "long_tone": [],
        "hi_low_tone": [],
        "mdc": [],
        "dtmf": [],
    }

    cfg = tone_detect_config or {}

    # Map config -> tone_detect kwargs with sane defaults from your new version
    params = dict(
        matching_threshold=float(cfg.get("matching_threshold", 2.5)),
        time_resolution_ms=int(cfg.get("time_resolution_ms", 50)),
        tone_a_min_length=float(cfg.get("tone_a_min_length", 0.85)),
        tone_b_min_length=float(cfg.get("tone_b_min_length", 2.6)),
        hi_low_interval=float(cfg.get("hi_low_interval", 0.2)),
        hi_low_min_alternations=int(cfg.get("hi_low_min_alternations", 6)),
        long_tone_min_length=float(cfg.get("long_tone_min_length", 3.8)),
        detect_mdc=bool(cfg.get("detect_mdc", False)),
        mdc_high_pass=int(cfg.get("mdc_high_pass", 200)),
        mdc_low_pass=int(cfg.get("mdc_low_pass", 4000)),
        detect_dtmf=bool(cfg.get("detect_dtmf", False)),
        debug=bool(cfg.get("debug", False)),
    )

    try:
        # BytesIO is supported by tone_detect via load_audio()
        with io.BytesIO(wav_data) as bio:
            results = tone_detect(bio, **params)

        # Normalize into your expected dict
        detected_tones.update({
            "two_tone": results.two_tone_result or [],
            "long_tone": results.long_result or [],
            "hi_low_tone": results.hi_low_result or [],
            "mdc": results.mdc_result or [],
            "dtmf": results.dtmf_result or [],
        })

    except Exception as e:
        # If you imported specific exceptions above, you can branch on type here.
        traceback.print_exc()
        if module_logger:
            module_logger.exception(f"Tone Detect - Error: {e}")

    return detected_tones
