from __future__ import annotations

import re
from email.message import EmailMessage
from email.utils import parsedate_to_datetime
from pathlib import Path

from getemails.filters import FilterSpec


def query_folder_name(spec: FilterSpec) -> str:
    """
    Build a human-readable folder name from a FilterSpec.
    Examples:
      2024-01-01_2024-03-31
      from-boss@co.com__to-me@co.com
      2024-01-01_2024-03-31__from-boss@co.com
      all  (no filters)
    """
    parts: list[str] = []

    if spec.since or spec.until:
        since = spec.since.strftime("%Y-%m-%d") if spec.since else "start"
        until = spec.until.strftime("%Y-%m-%d") if spec.until else "end"
        parts.append(f"{since}_{until}")

    if spec.senders:
        parts.append("from-" + "+".join(spec.senders))

    if spec.recipients:
        parts.append("to-" + "+".join(spec.recipients))

    raw = "__".join(parts) if parts else "all"
    # Sanitize for filesystem safety
    return re.sub(r"[^\w@.+_-]", "-", raw)


def _slugify(text: str, max_len: int = 60) -> str:
    text = re.sub(r"[^\w\s-]", "", text, flags=re.ASCII)
    text = re.sub(r"[\s]+", "-", text.strip())
    return text[:max_len]


def _message_uid(msg: EmailMessage) -> str:
    """Best-effort stable ID: Message-ID header, stripped of angle brackets."""
    mid = msg.get("Message-ID", "")
    return re.sub(r"[<>\s]", "", mid) or "unknown"


def _message_filename(msg: EmailMessage) -> str:
    """
    Format: {date}_{time}__{uid}__{subject-slug}.eml
    Example: 2024-03-15_143022__abc123def456__Re-project-update.eml
    """
    try:
        dt = parsedate_to_datetime(msg.get("Date", ""))
        date_part = dt.strftime("%Y-%m-%d_%H%M%S")
    except Exception:
        date_part = "0000-00-00_000000"

    uid = _message_uid(msg)[:32]
    subject = _slugify(msg.get("Subject", "no-subject"))
    return f"{date_part}__{uid}__{subject}.eml"


def save_eml(msg: EmailMessage, output_dir: Path) -> Path | None:
    """
    Write msg to output_dir as a .eml file.
    Returns the path written, or None if the message was already present.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    uid = _message_uid(msg)

    if uid != "unknown" and any(output_dir.glob(f"*__{uid}__*.eml")):
        return None

    filename = _message_filename(msg)
    path = output_dir / filename
    path.write_bytes(msg.as_bytes())
    return path