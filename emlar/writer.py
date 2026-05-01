from __future__ import annotations

import email
import email.policy
from email.message import EmailMessage
from pathlib import Path

from emlar.sorting import SortingSpec
from emlar.db import init_db, query_messages
from emlar.email_utils import message_filename, message_uid, slugify, stream_emls, stream_mbox, thread_id, thread_subject, message_date
from emlar.filters import FilterSpec


def _resolve_target_dir(
    msg: EmailMessage,
    output_dir: Path,
    sorting_spec: SortingSpec,
    folder: str | None,
    thread_subjects: dict[str, str],
) -> Path:
    target = output_dir

    if sorting_spec.groupby_date:
        target = target / message_date(msg)
    if sorting_spec.groupby_folder and folder:
        target = target / slugify(folder)
    if sorting_spec.groupby_thread:
        tid = thread_id(msg)
        if tid not in thread_subjects:
            thread_subjects[tid] = thread_subject(msg)
        target = target / thread_subjects[tid]

    return target


def write_emls(
    db_path: Path,
    output_dir: Path,
    filter_spec: FilterSpec,
    sorting_spec: SortingSpec | None = None,
) -> tuple[int, int]:
    """
    Write .eml files to output_dir from the fetch database.
    Returns (saved, skipped) counts.
    """
    sorting = sorting_spec or SortingSpec()
    thread_subjects: dict[str, str] = {}
    saved = skipped = 0

    conn = init_db(db_path)
    rows = query_messages(conn, filter_spec, sorting)
    conn.close()

    for row in rows:
        msg = email.message_from_bytes(bytes(row["raw"]), policy=email.policy.default)
        target_dir = _resolve_target_dir(msg, output_dir, sorting, row["folder"], thread_subjects)
        target_dir.mkdir(parents=True, exist_ok=True)

        uid = message_uid(msg)
        if uid != "unknown" and any(output_dir.glob(f"**/*__{uid}__*.eml")):
            skipped += 1
            continue

        path = target_dir / message_filename(msg)
        path.write_bytes(bytes(row["raw"]))
        saved += 1

    return saved, skipped

def write_mbox(
    db_path: Path,
    output_path: Path,
    filter_spec: FilterSpec,
) -> tuple[int, int]:
    """Write messages from the database into a single .mbox file."""
    import mailbox

    conn = init_db(db_path)
    rows = query_messages(conn, filter_spec)
    conn.close()

    mbox = mailbox.mbox(str(output_path))
    mbox.lock()
    saved = skipped = 0

    try:
        seen_uids: set[str] = set()
        for row in rows:
            uid = row["message_id"]
            if uid != "unknown" and uid in seen_uids:
                skipped += 1
                continue
            msg = email.message_from_bytes(bytes(row["raw"]), policy=email.policy.default)
            if row["folder"] and "X-Folder" not in msg:
                msg["X-Folder"] = row["folder"]
            mbox.add(msg)
            seen_uids.add(uid)
            saved += 1
    finally:
        mbox.flush()
        mbox.unlock()

    return saved, skipped