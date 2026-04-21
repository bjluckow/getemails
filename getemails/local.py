from __future__ import annotations

import email
import email.policy
from pathlib import Path
from typing import Iterator

from getemails.filters import FilterSpec
from getemails.storage import save_eml


def stream_emls(input_dir: Path, recursive: bool) -> Iterator[Path]:
    """Yield .eml file paths from input_dir, optionally recursing subdirs."""
    pattern = "**/*.eml" if recursive else "*.eml"
    yield from sorted(input_dir.glob(pattern))


def filter_local(
    input_dir: Path,
    output_dir: Path,
    spec: FilterSpec,
    recursive: bool,
) -> tuple[int, int, int]:
    """
    Stream .eml files from input_dir, apply spec, write matches to output_dir.
    Returns (saved, skipped, filtered) counts.
    """
    saved = skipped = filtered = 0

    for eml_path in stream_emls(input_dir, recursive):
        with eml_path.open("rb") as f:
            msg = email.message_from_binary_file(f, policy=email.policy.default)

        if not spec.is_empty() and not spec.matches(msg):
            filtered += 1
            continue

        path = save_eml(msg, output_dir)
        if path:
            saved += 1
        else:
            skipped += 1

    return saved, skipped, filtered