from __future__ import annotations

from pathlib import Path
import re
import email
import email.policy
import mailbox
from email.message import EmailMessage
from email.utils import getaddresses, parsedate_to_datetime
from typing import Iterator


def slugify(text: str, max_len: int = 60) -> str:
    text = re.sub(r"[^\w\s-]", "", text, flags=re.ASCII)
    text = re.sub(r"[\s]+", "-", text.strip())
    return text[:max_len]


def message_uid(msg: EmailMessage) -> str:
    """Best-effort stable ID: Message-ID header, stripped of angle brackets."""
    mid = msg.get("Message-ID", "")
    return re.sub(r"[<>\s]", "", mid) or "unknown"


def message_date(msg: EmailMessage) -> str:
    """Return YYYY-MM-DD string for the message date."""
    try:
        dt = parsedate_to_datetime(msg.get("Date", ""))
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return "0000-00-00"


def message_filename(msg: EmailMessage) -> str:
    """
    Return a filesystem-safe filename for the message.
    Format: {date}_{time}__{uid}__{subject-slug}.eml
    """
    try:
        dt = parsedate_to_datetime(msg.get("Date", ""))
        date_part = dt.strftime("%Y-%m-%d_%H%M%S")
    except Exception:
        date_part = "0000-00-00_000000"

    uid = message_uid(msg)[:32]
    subject = slugify(msg.get("Subject", "no-subject"))
    return f"{date_part}__{uid}__{subject}.eml"


def thread_id(msg: EmailMessage) -> str:
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

    return message_uid(msg)


def thread_subject(msg: EmailMessage) -> str:
    """
    Return a filesystem-safe thread subject, stripping Re:/Fwd: prefixes.
    Used as the thread directory name.
    """
    subject = msg.get("Subject", "no-subject")
    subject = re.sub(
        r"^(re|fwd?|fw)\s*:\s*", "", subject, flags=re.IGNORECASE
    ).strip()
    return slugify(subject) or "no-subject"


def extract_addrs(msg: EmailMessage, header: str) -> str | None:
    """
    Extract all addresses from a header field, returned as a
    comma-separated lowercase string. Returns None if no addresses found.
    """
    raw = msg.get_all(header, [])
    addrs = [addr.lower() for _, addr in getaddresses(raw) if addr]
    return ",".join(addrs) or None


def stream_emls(input_dir: Path, recursive: bool) -> Iterator[EmailMessage]:
    pattern = "**/*.eml" if recursive else "*.eml"
    for path in sorted(input_dir.glob(pattern)):
        with path.open("rb") as f:
            yield email.message_from_binary_file(f, policy=email.policy.default)


def stream_mbox(mbox_path: Path) -> Iterator[EmailMessage]:
    mbox = mailbox.mbox(str(mbox_path), factory=None, create=False)
    try:
        for raw_msg in mbox:
            yield email.message_from_bytes(
                raw_msg.as_bytes(), policy=email.policy.default
            )
    finally:
        mbox.close()