# lib/liquidsoap_handler.py  (drop-in replacement or update the functions below)
from __future__ import annotations
import logging, os, shutil, socket, threading, time
from typing import Dict, Any, Optional

module_logger = logging.getLogger("icad_tr_consumer.liquidsoap_handler")

def _fmt(v): return "" if v is None else str(v)
def _escape_liq_annot(v: str) -> str: return _fmt(v).replace("'", r"\'")

class _SafeDict(dict):
    def __missing__(self, key): return ""

def _render_meta(template: str, call_data: Dict[str, Any]) -> str:
    safe = dict(call_data or {})
    tg = call_data.get("talkgroup")
    tg_alpha = call_data.get("talkgroup_alpha_tag") or ""
    safe["fallbackTG"] = "" if tg_alpha else ("" if tg is None else f" (TG {tg})")
    try:
        return template.format_map(_SafeDict(safe))
    except Exception:
        return template

def _send_liquidsoap_cmd(host: str, port: int, cmd: str, password: Optional[str] = None, timeout: float = 3.5) -> str:
    with socket.create_connection((host, port), timeout=timeout) as s:
        s.settimeout(timeout)
        try: banner = s.recv(4096).decode(errors="ignore")
        except Exception: banner = ""
        if "Password" in banner and password:
            s.sendall((password + "\n").encode())
            try: _ = s.recv(4096)
            except Exception: pass
        s.sendall((cmd + "\n").encode())
        chunks, t0 = [], time.time()
        while time.time() - t0 < timeout:
            try:
                buf = s.recv(4096)
                if not buf: break
                chunks.append(buf)
                if b"> " in buf or b"END" in buf: break
            except socket.timeout:
                break
            except Exception:
                break
        return b"".join(chunks).decode(errors="ignore")

def _resolve_source_path(temp_dir: str, filename: str, prefer: str) -> Optional[str]:
    """
    Accepts filename WITH or WITHOUT extension.
    Prefer the configured extension first (e.g., m4a) so we use the loudnormed version.
    """
    prefer = (prefer or "wav").lower()
    order = [".wav", ".m4a"] if prefer == "wav" else [".m4a", ".wav"]

    base = os.path.basename(filename or "")
    base_no_ext, ext = os.path.splitext(base)

    candidates = []
    # 1) Try preferred extension first, then the alternate
    for e in order:
        candidates.append(os.path.join(temp_dir, base_no_ext + e))
    # 2) Finally, try the exact path as provided (in case it's something custom)
    if ext:
        candidates.append(os.path.join(temp_dir, base))

    # De-dup, then pick the first that exists
    seen, deduped = set(), []
    for p in candidates:
        if p not in seen:
            seen.add(p); deduped.append(p)

    for p in deduped:
        if os.path.isfile(p):
            return p

    module_logger.debug(f"[liquidsoap] Tried paths (none existed): {deduped}")
    return None

def _schedule_delete(path: str, delay: float):
    def _worker():
        try:
            time.sleep(max(1.0, delay))
            if os.path.exists(path):
                os.remove(path)
                module_logger.debug(f"[liquidsoap] Staged file removed: {path}")
        except Exception:
            module_logger.exception("[liquidsoap] Failed to remove staged file")
    threading.Thread(target=_worker, daemon=True).start()

def upload_to_broadcastify_icecast(ls_cfg: Dict[str, Any], temp_dir: str, call_data: Dict[str, Any]) -> bool:
    if not ls_cfg or not ls_cfg.get("enabled"): return False

    host = ls_cfg.get("host", "127.0.0.1")
    port = int(ls_cfg.get("port", 1234))
    password = (ls_cfg.get("password") or None)
    queue_id = ls_cfg.get("queue_id", "icad")
    staging_dir = ls_cfg.get("staging_dir", "/var/spool/liquidsoap/icad")
    prefer = (ls_cfg.get("prefer_source") or "wav").lower()
    extra_del = float(ls_cfg.get("delete_after_seconds", 0))

    base = call_data.get("filename")
    if not base:
        module_logger.warning("[liquidsoap] call_data.filename missing; cannot enqueue")
        return False

    src_path = _resolve_source_path(temp_dir, base, prefer)
    if not src_path:
        module_logger.warning(f"[liquidsoap] No source file found for {base} (looked for WAV/M4A).")
        return False

    os.makedirs(staging_dir, exist_ok=True)
    # Build a stable staged name
    instance = _fmt(call_data.get("instance_id")) or _fmt(call_data.get("start_time")) or _fmt(int(time.time()))
    staged_name = f"{instance}_{os.path.basename(src_path)}"
    staged_path = os.path.join(staging_dir, staged_name)

    try:
        shutil.copy2(src_path, staged_path)
        module_logger.debug(f"[liquidsoap] Staged file: {staged_path}")
    except Exception:
        module_logger.exception(f"[liquidsoap] Failed to stage file: {staged_path}")
        return False

    # Metadata
    md_cfg = (ls_cfg.get("metadata") or {})
    title   = _render_meta(md_cfg.get("title",   "{talkgroup_alpha_tag}{fallbackTG}"), call_data)
    artist  = _render_meta(md_cfg.get("artist",  "{short_name}"), call_data)
    album   = _render_meta(md_cfg.get("album",   "{system}"), call_data)
    genre   = _render_meta(md_cfg.get("genre",   "Public Safety"), call_data)
    comment = _render_meta(md_cfg.get("comment", "TG {talkgroup} | Len {call_length:.1f}s"), call_data)

    tgid   = _fmt(call_data.get("talkgroup"))
    system = _fmt(call_data.get("short_name"))
    length = _fmt(call_data.get("call_length"))
    site   = _fmt(call_data.get("site"))
    rid = ""
    try:
        src_list = call_data.get("srcList") or []
        if isinstance(src_list, list) and src_list:
            rid = _fmt(src_list[0].get("src"))
    except Exception:
        pass

    annotate_pairs = {
        "title": title, "artist": artist, "album": album, "genre": genre, "comment": comment,
        "tgid": tgid, "system": system, "site": site, "rid": rid, "len": length,
    }
    kv = ",".join(f"{k}='{_escape_liq_annot(v)}'" for k, v in annotate_pairs.items() if _fmt(v) != "")
    annotate_uri = f"annotate:{kv}:{staged_path}"

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

    call_len = float(call_data.get("call_length") or 0.0)
    delay = (call_len if call_len > 0 else 90.0) + 20.0 + max(0.0, extra_del)
    _schedule_delete(staged_path, delay)

    return ok
