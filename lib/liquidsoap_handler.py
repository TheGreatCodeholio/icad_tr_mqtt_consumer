# lib/liquidsoap_handler.py
"""
Enqueue per-call audio into a Liquidsoap request queue which is configured
to stream to Broadcastify's Icecast endpoint.

Flow:
- Copy the temp WAV/M4A into a shared "staging_dir" visible to the Liquidsoap host.
- Push an annotated request for that file to Liquidsoap over its telnet server.
- (Optional) Schedule deletion of the staged file after it should have been played.

Config example (in your system config under each system):
"broadcastify_icecast": {
  "enabled": 1,
  "host": "127.0.0.1",
  "port": 1234,
  "password": "changeme",          # omit or "" if you don't use a telnet password
  "queue_id": "icad",              # matches request.queue(id='icad') in .liq
  "staging_dir": "/var/spool/liquidsoap/icad",
  "prefer_source": "wav",          # "wav" or "m4a"; falls back automatically
  "delete_after_seconds": 900,     # optional extra safety cleanup
  "metadata": {
    "title": "{talkgroup_alpha_tag}{fallbackTG}",
    "artist": "{short_name}",
    "album": "{system}",
    "genre": "Public Safety",
    "comment": "TG {talkgroup} | Len {call_length:.1f}s"
  }
}
"""

from __future__ import annotations

import logging
import os
import shutil
import socket
import threading
import time
from typing import Dict, Any, Optional

module_logger = logging.getLogger("icad_tr_consumer.liquidsoap_handler")

def _first_existing(paths):
    for p in paths:
        if p and os.path.isfile(p):
            return p
    return None

def _fmt(val) -> str:
    return "" if val is None else str(val)

def _escape_liq_annot(v: str) -> str:
    # Liquidsoap annotate syntax uses single quotes for values; escape ' as \'
    return _fmt(v).replace("'", r"\'")

def _render_meta(template: str, call_data: Dict[str, Any]) -> str:
    # Provide a few helpful fallbacks for templates
    # fallbackTG renders " (TG 1234)" if alpha tag missing
    safe = {k: v for k, v in (call_data or {}).items()}
    tg = call_data.get("talkgroup")
    tg_alpha = call_data.get("talkgroup_alpha_tag") or ""
    safe["fallbackTG"] = "" if tg_alpha else ("" if tg is None else f" (TG {tg})")
    # Format floats nicely if used
    try:
        return template.format_map(_SafeDict(safe))
    except Exception:
        return template  # on template error, just return as-is

class _SafeDict(dict):
    def __missing__(self, key):
        return ""

def _send_liquidsoap_cmd(host: str, port: int, cmd: str, password: Optional[str] = None, timeout: float = 3.5) -> str:
    """
    Minimal telnet-like client for Liquidsoap server.
    Reads the greeting, handles optional password prompt, runs one command, returns the raw response.
    """
    with socket.create_connection((host, port), timeout=timeout) as s:
        s.settimeout(timeout)
        # read banner / optional password prompt
        try:
            banner = s.recv(4096).decode(errors="ignore")
        except Exception:
            banner = ""
        if "Password" in banner and password:
            s.sendall((password + "\n").encode())
            try:
                _ = s.recv(4096)  # consume post-auth prompt
            except Exception:
                pass
        # send the command
        s.sendall((cmd + "\n").encode())
        # read until prompt or timeout
        chunks = []
        t0 = time.time()
        while time.time() - t0 < timeout:
            try:
                buf = s.recv(4096)
                if not buf:
                    break
                chunks.append(buf)
                if b"> " in buf or b"END" in buf:
                    break
            except socket.timeout:
                break
            except Exception:
                break
        return b"".join(chunks).decode(errors="ignore")

def _schedule_delete(path: str, delay: float):
    def _worker():
        try:
            time.sleep(max(1.0, delay))
            if os.path.exists(path):
                os.remove(path)
                module_logger.debug(f"[liquidsoap] Staged file removed: {path}")
        except Exception:
            module_logger.exception("[liquidsoap] Failed to remove staged file")
    th = threading.Thread(target=_worker, daemon=True)
    th.start()

def upload_to_broadcastify_icecast(ls_cfg: Dict[str, Any],
                                   temp_dir: str,
                                   call_data: Dict[str, Any]) -> bool:
    """
    Main entrypoint: enqueue current call into Liquidsoap queue.
    Returns True if pushing to queue seemed successful.
    """
    if not ls_cfg or not ls_cfg.get("enabled"):
        return False

    host = ls_cfg.get("host", "127.0.0.1")
    port = int(ls_cfg.get("port", 1234))
    password = (ls_cfg.get("password") or None)
    queue_id = ls_cfg.get("queue_id", "icad")
    staging_dir = ls_cfg.get("staging_dir", "/var/spool/liquidsoap/icad")
    prefer = (ls_cfg.get("prefer_source") or "wav").lower()
    extra_del = float(ls_cfg.get("delete_after_seconds", 0))  # optional

    base = call_data.get("filename")
    if not base:
        module_logger.warning("[liquidsoap] call_data.filename missing; cannot enqueue")
        return False

    # Build candidate source paths from temp_dir
    src_wav = os.path.join(temp_dir, f"{base}.wav")
    src_m4a = os.path.join(temp_dir, f"{base}.m4a")
    candidates = [src_wav, src_m4a] if prefer == "wav" else [src_m4a, src_wav]
    src_path = _first_existing(candidates)
    if not src_path:
        module_logger.warning(f"[liquidsoap] No source file found for {base} (looked for WAV/M4A).")
        return False

    # Ensure staging dir exists and stage the file (copy to avoid temp cleanup races)
    os.makedirs(staging_dir, exist_ok=True)
    # Make a clear, unique staged filename
    instance = _fmt(call_data.get("instance_id")) or _fmt(call_data.get("start_time")) or _fmt(int(time.time()))
    ext = os.path.splitext(src_path)[1].lower() or ".wav"
    staged_name = f"{instance}_{base}{ext}"
    staged_path = os.path.join(staging_dir, staged_name)

    try:
        shutil.copy2(src_path, staged_path)
    except Exception:
        module_logger.exception(f"[liquidsoap] Failed to stage file: {staged_path}")
        return False

    # Build metadata (annotate:...:path)
    md_cfg: Dict[str, str] = (ls_cfg.get("metadata") or {})
    title = _render_meta(md_cfg.get("title", "{talkgroup_alpha_tag}{fallbackTG}"), call_data)
    artist = _render_meta(md_cfg.get("artist", "{short_name}"), call_data)
    album = _render_meta(md_cfg.get("album", "{system}"), call_data)
    genre = _render_meta(md_cfg.get("genre", "Public Safety"), call_data)
    comment = _render_meta(md_cfg.get("comment", "TG {talkgroup} | Len {call_length:.1f}s"), call_data)

    # Extra useful raw fields
    tgid = _fmt(call_data.get("talkgroup"))
    system = _fmt(call_data.get("short_name"))
    length = _fmt(call_data.get("call_length"))
    site = _fmt(call_data.get("site"))
    rid = ""
    try:
        # some call payloads have srcList -> array of RIDs; use first if present
        src_list = call_data.get("srcList") or []
        if isinstance(src_list, list) and src_list:
            rid = _fmt(src_list[0].get("src"))
    except Exception:
        pass

    annotate_pairs = {
        "title": title,
        "artist": artist,
        "album": album,
        "genre": genre,
        "comment": comment,
        "tgid": tgid,
        "system": system,
        "site": site,
        "rid": rid,
        "len": length,
    }
    # Construct annotate string
    kv = ",".join(f"{k}='{_escape_liq_annot(v)}'" for k, v in annotate_pairs.items() if _fmt(v) != "")
    annotate_uri = f"annotate:{kv}:{staged_path}"

    # Command: '<queue_id>.push annotate:...:/path/file'
    cmd = f"{queue_id}.push {annotate_uri}"
    try:
        resp = _send_liquidsoap_cmd(host, port, cmd, password=password)
        ok = ("Done" in resp) or ("queued" in resp) or ("request" in resp and "added" in resp) or ("> " in resp)
        if ok:
            module_logger.info(f"[liquidsoap] Enqueued -> {os.path.basename(staged_path)}")
        else:
            module_logger.warning(f"[liquidsoap] Unexpected response pushing request: {resp.strip()}")
    except Exception:
        module_logger.exception("[liquidsoap] Failed to push request to Liquidsoap")
        ok = False

    # Optional cleanup of staged file after it *should* be played
    # Use call_length with a small buffer; if call_length missing, use 90s default.
    call_len = float(call_data.get("call_length") or 0.0)
    delay = (call_len if call_len > 0 else 90.0) + 20.0 + max(0.0, extra_del)
    _schedule_delete(staged_path, delay)

    return ok
