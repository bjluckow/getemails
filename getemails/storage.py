from __future__ import annotations

import re
from dataclasses import dataclass
from email.message import EmailMessage
from email.utils import parsedate_to_datetime
from pathlib import Path

from getemails.filters import FilterSpec


@dataclass
class GroupBy:
    date: bool = False
    thread: bool = False


def query_folder_name(spec: FilterSpec) -> str:
    parts: list[str] = []

    if spec.since or spec.until:
        since = spec.since.strftime("%Y-%m-%d") if spec.since else "start"
        until = spec.until.strftime("%Y-%m-%d") if spec.until else "end"
        parts.append(f"{since}_{until}")

    if spec.senders:
        parts.append("from-" + "+".join(spec.senders))
    if spec.recipients:
        parts.append("to-" + "+".join(spec.recipients))
    if spec.cc:
        parts.append("cc-" + "+".join(spec.cc))
    if spec.bcc:
        parts.append("bcc-" + "+".join(spec.bcc))
    if spec.any_addresses:
        parts.append("any-" + "+".join(spec.any_addresses))

    raw = "__".join(parts) if parts else "all"
    return re.sub(r"[^\w@.+_-]", "-", raw)


def _slugify(text: str, max_len: int = 60) -> str:
    text = re.sub(r"[^\w\s-]", "", text, flags=re.ASCII)
    text = re.sub(r"[\s]+", "-", text.strip())
    return text[:max_len]


def _message_uid(msg: EmailMessage) -> str:
    mid = msg.get("Message-ID", "")
    return re.sub(r"[<>\s]", "", mid) or "unknown"


def _message_filename(msg: EmailMessage) -> str:
    try:
        dt = parsedate_to_datetime(msg.get("Date", ""))
        date_part = dt.strftime("%Y-%m-%d_%H%M%S")
    except Exception:
        date_part = "0000-00-00_000000"

    uid = _message_uid(msg)[:32]
    subject = _slugify(msg.get("Subject", "no-subject"))
    return f"{date_part}__{uid}__{subject}.eml"


def _message_date_dir(msg: EmailMessage) -> str:
    """Return YYYY-MM-DD string for the message date."""
    try:
        dt = parsedate_to_datetime(msg.get("Date", ""))
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return "0000-00-00"


def _thread_id(msg: EmailMessage) -> str:
    """
    Return the root Message-ID for the thread.
    Uses References first (first entry is the root), then In-Reply-To,
    then falls back to the message's own Message-ID.
    """
    references = msg.get("References", "").strip()
    if references:
        root = references.split()[0]
        return re.sub(r"[<>\s]", "", root)

    in_reply_to = msg.get("In-Reply-To", "").strip()
    if in_reply_to:
        return re.sub(r"[<>\s]", "", in_reply_to)

    return _message_uid(msg)


def _thread_subject(msg: EmailMessage) -> str:
    """
    Return a filesystem-safe thread subject, stripping Re:/Fwd: prefixes.
    Used as the thread directory name.
    """
    subject = msg.get("Subject", "no-subject")
    # Strip Re:, Fwd:, Fw: prefixes (case-insensitive, repeated)
    subject = re.sub(r"^(re|fwd?|fw)\s*:\s*", "", subject, flags=re.IGNORECASE).strip()
    return _slugify(subject) or "no-subject"


# Thread subject cache — maps thread_id -> subject slug
# Populated as messages are saved so the first downloaded message
# in a thread sets the name for all subsequent ones.
_thread_subjects: dict[str, str] = {}


def _resolve_thread_dir(msg: EmailMessage) -> str:
    """Return the thread directory name for this message."""
    tid = _thread_id(msg)
    if tid not in _thread_subjects:
        _thread_subjects[tid] = _thread_subject(msg)
    return _thread_subjects[tid]


def save_eml(
    msg: EmailMessage,
    output_dir: Path,
    group_by: GroupBy | None = None,
) -> Path | None:
    """
    Write msg to output_dir as a .eml file, optionally grouped by date and/or thread.
    Returns the path written, or None if the message was already present.
    """
    target_dir = output_dir

    if group_by:
        if group_by.date:
            target_dir = target_dir / _message_date_dir(msg)
        if group_by.thread:
            target_dir = target_dir / _resolve_thread_dir(msg)

    target_dir.mkdir(parents=True, exist_ok=True)
    uid = _message_uid(msg)

    # Dedup check — search recursively from output_dir so we catch
    # messages already saved under a different grouping path
    if uid != "unknown" and any(output_dir.glob(f"**/*__{uid}__*.eml")):
        return None

    filename = _message_filename(msg)
    path = target_dir / filename
    path.write_bytes(msg.as_bytes())
    return path