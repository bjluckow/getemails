from __future__ import annotations

import email
import email.policy
import mailbox
from email.message import EmailMessage
from pathlib import Path
from typing import Iterator

from getemails.filters import FilterSpec
from getemails.storage import save_eml


def stream_emls(input_dir: Path, recursive: bool) -> Iterator[EmailMessage]:
    """Yield EmailMessages from .eml files in input_dir."""
    pattern = "**/*.eml" if recursive else "*.eml"
    for path in sorted(input_dir.glob(pattern)):
        with path.open("rb") as f:
            yield email.message_from_binary_file(f, policy=email.policy.default)


def stream_mbox(mbox_path: Path) -> Iterator[EmailMessage]:
    """Yield EmailMessages from an mbox file one at a time."""
    mbox = mailbox.mbox(str(mbox_path), factory=None, create=False)
    try:
        for raw_msg in mbox:
            yield email.message_from_bytes(
                raw_msg.as_bytes(), policy=email.policy.default
            )
    finally:
        mbox.close()


def filter_local(
    input_path: Path,
    output_dir: Path,
    spec: FilterSpec,
    recursive: bool = False,
    mbox: bool = False,
) -> tuple[int, int, int]:
    """
    Stream messages from input_path, apply spec, write matches to output_dir.
    input_path is either a directory of .eml files or an .mbox file.
    Returns (saved, skipped, filtered) counts.
    """
    saved = skipped = filtered = 0

    source = stream_mbox(input_path) if mbox else stream_emls(input_path, recursive)

    for msg in source:
        if not spec.is_empty() and not spec.matches(msg):
            filtered += 1
            continue
        path = save_eml(msg, output_dir)
        if path:
            saved += 1
        else:
            skipped += 1

    return saved, skipped, filtered