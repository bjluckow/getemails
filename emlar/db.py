from __future__ import annotations

from dataclasses import dataclass
from email.message import EmailMessage
import sqlite3
from pathlib import Path
from typing import Iterator

from emlar.filters import FilterSpec
from emlar.sorting import SortingSpec

# --- schema ------------------------------------------------------------------

_CREATE_MESSAGES = """
    CREATE TABLE IF NOT EXISTS messages (
        message_id  TEXT PRIMARY KEY,
        account     TEXT NOT NULL,
        folder      TEXT NOT NULL,
        date        TEXT,
        subject     TEXT,
        from_addr   TEXT,
        to_addr     TEXT,
        cc_addr     TEXT,
        bcc_addr    TEXT,
        raw         BLOB NOT NULL
    )
"""

_CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_date       ON messages (date)",
    "CREATE INDEX IF NOT EXISTS idx_account    ON messages (account)",
    "CREATE INDEX IF NOT EXISTS idx_folder     ON messages (folder)",
    "CREATE INDEX IF NOT EXISTS idx_from_addr  ON messages (from_addr)",
    "CREATE INDEX IF NOT EXISTS idx_to_addr    ON messages (to_addr)",
]

# --- queries -----------------------------------------------------------------

_INSERT_MESSAGE = """
    INSERT OR IGNORE INTO messages
        (message_id, account, folder, date, subject,
         from_addr, to_addr, cc_addr, bcc_addr, raw)
    VALUES
        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

# Base SELECT — WHERE clauses appended dynamically by query_messages
_SELECT_MESSAGES = "SELECT message_id, account, folder, date, subject, from_addr, to_addr, cc_addr, bcc_addr, raw FROM messages"

# --- connection --------------------------------------------------------------

def init_db(db_path: Path) -> sqlite3.Connection:
    """
    Open (or create) the SQLite database at db_path.
    Creates tables and indexes if they don't exist.
    Returns an open connection — caller is responsible for closing.
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")  # safe for concurrent writes
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(_CREATE_MESSAGES)
    for idx in _CREATE_INDEXES:
        conn.execute(idx)
    conn.commit()
    return conn

# --- writes ------------------------------------------------------------------

def insert_message(
    conn: sqlite3.Connection,
    message_id: str,
    account: str,
    folder: str,
    date: str | None,
    subject: str | None,
    from_addr: str | None,
    to_addr: str | None,
    cc_addr: str | None,
    bcc_addr: str | None,
    raw: bytes,
) -> None:
    conn.execute(_INSERT_MESSAGE, (
        message_id, account, folder, date, subject,
        from_addr, to_addr, cc_addr, bcc_addr, raw,
    ))
    conn.commit()


def insert_from_stream(
    conn: sqlite3.Connection,
    messages: Iterator[EmailMessage],
    account: str,
    folder: str,
    spec: FilterSpec | None = None,
) -> tuple[int, int]:
    """
    Insert messages from an iterator into the DB.
    Returns (inserted, filtered) counts.
    """
    from emlar.email_utils import extract_addrs, message_date, message_uid
    inserted = filtered = 0
    for msg in messages:
        if spec and not spec.is_empty() and not spec.matches(msg):
            filtered += 1
            continue
        uid = message_uid(msg)
        insert_message(
            conn,
            message_id=uid,
            account=account,
            folder=folder,
            date=message_date(msg),
            subject=msg.get("Subject"),
            from_addr=extract_addrs(msg, "From"),
            to_addr=extract_addrs(msg, "To"),
            cc_addr=extract_addrs(msg, "Cc"),
            bcc_addr=extract_addrs(msg, "Bcc"),
            raw=msg.as_bytes(),
        )
        inserted += 1
    return inserted, filtered


# --- reads -------------------------------------------------------------------

def _build_where(spec: FilterSpec | None) -> tuple[str, list]:
    """Build a WHERE clause and params list from a FilterSpec."""
    clauses: list[str] = []
    params: list = []

    if spec:
        if spec.since:
            clauses.append("date >= ?")
            params.append(spec.since.strftime("%Y-%m-%d"))
        if spec.until:
            clauses.append("date < ?")
            params.append(spec.until.strftime("%Y-%m-%d"))
        if spec.senders:
            clauses.append(f"({' OR '.join('from_addr LIKE ?' for _ in spec.senders)})")
            params.extend(f"%{s.lower()}%" for s in spec.senders)
        if spec.recipients:
            clauses.append(f"({' OR '.join('to_addr LIKE ?' for _ in spec.recipients)})")
            params.extend(f"%{r.lower()}%" for r in spec.recipients)
        if spec.cc:
            clauses.append(f"({' OR '.join('cc_addr LIKE ?' for _ in spec.cc)})")
            params.extend(f"%{c.lower()}%" for c in spec.cc)
        if spec.bcc:
            clauses.append(f"({' OR '.join('bcc_addr LIKE ?' for _ in spec.bcc)})")
            params.extend(f"%{b.lower()}%" for b in spec.bcc)
        if spec.any_addresses:
            any_clauses = []
            for addr in spec.any_addresses:
                any_clauses.append(
                    "(from_addr LIKE ? OR to_addr LIKE ? OR cc_addr LIKE ? OR bcc_addr LIKE ?)"
                )
                params.extend([f"%{addr.lower()}%"] * 4)
            clauses.append(f"({' OR '.join(any_clauses)})")

    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


def query_messages(
    conn: sqlite3.Connection,
    spec: FilterSpec | None = None,
    sorting_spec: SortingSpec | None = None,
) -> list[sqlite3.Row]:
    where, params = _build_where(spec)

    order_parts = ["date"]
    if sorting_spec and sorting_spec.groupby_folder:
        order_parts.append("folder")
    order_parts.append("subject")

    sql = f"{_SELECT_MESSAGES}{where} ORDER BY {', '.join(order_parts)}"
    return conn.execute(sql, params).fetchall()

def delete_messages(
    conn: sqlite3.Connection,
    spec: FilterSpec | None = None,
) -> int:
    where, params = _build_where(spec)
    sql = f"DELETE FROM messages{where}"
    cursor = conn.execute(sql, params)
    conn.commit()
    return cursor.rowcount

# --- stats -------------------------------------------------------------------

@dataclass
class FolderStats:
    folder: str
    count: int
    earliest: str | None
    latest: str | None

@dataclass
class AccountStats:
    account: str
    count: int
    earliest: str | None
    latest: str | None
    folders: list[FolderStats]

_GET_STATS_BY_ACCOUNT = """
    SELECT
        account,
        COUNT(*) as count,
        MIN(date) as earliest,
        MAX(date) as latest
    FROM messages
    GROUP BY account
    ORDER BY count DESC
"""

_GET_STATS_BY_FOLDER = """
    SELECT
        account,
        folder,
        COUNT(*) as count,
        MIN(date) as earliest,
        MAX(date) as latest
    FROM messages
    GROUP BY account, folder
    ORDER BY account, count DESC
"""

def get_stats(
    conn: sqlite3.Connection,
    spec: FilterSpec | None = None,
) -> list[AccountStats]:
    where, params = _build_where(spec)

    account_rows = conn.execute(
        f"SELECT account, COUNT(*) as count, MIN(date) as earliest, MAX(date) as latest "
        f"FROM messages{where} GROUP BY account ORDER BY count DESC",
        params,
    ).fetchall()

    folder_rows = conn.execute(
        f"SELECT account, folder, COUNT(*) as count, MIN(date) as earliest, MAX(date) as latest "
        f"FROM messages{where} GROUP BY account, folder ORDER BY account, count DESC",
        params,
    ).fetchall()

    folders_by_account: dict[str, list[FolderStats]] = {}
    for row in folder_rows:
        folders_by_account.setdefault(row["account"], []).append(
            FolderStats(folder=row["folder"], count=row["count"],
                        earliest=row["earliest"], latest=row["latest"])
        )

    return [
        AccountStats(account=row["account"], count=row["count"],
                     earliest=row["earliest"], latest=row["latest"],
                     folders=folders_by_account.get(row["account"], []))
        for row in account_rows
    ]