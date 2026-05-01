from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from emlar.db import init_db, insert_message
from emlar.email_utils import extract_addrs, message_date, message_uid
from emlar.filters import FilterSpec
from emlar.logger import AccountProgress, ProgressLogger, log
from emlar.providers.base import AccountConfig, EmailProvider


def make_provider(account: AccountConfig) -> EmailProvider:
    match account.provider:
        case "gmail":
            from emlar.providers.gmail import GmailProvider
            return GmailProvider(account)
        case "icloud":
            from emlar.providers.icloud import iCloudProvider
            return iCloudProvider(account)
        case "aol":
            from emlar.providers.aol import AOLProvider
            return AOLProvider(account)
        case _:
            raise ValueError(f"Unknown provider: {account.provider!r}")


def fetch_folders(account: AccountConfig) -> list[str]:
    provider = make_provider(account)
    with provider:
        from emlar.providers.imap import IMAPProvider
        from emlar.providers.gmail import GmailProvider
        if isinstance(provider, IMAPProvider):
            return provider._list_folders()
        elif isinstance(provider, GmailProvider):
            return provider.list_labels()
        return []


def fetch(
    account: AccountConfig,
    spec: FilterSpec,
    db_path: Path,
    progress: AccountProgress,
) -> tuple[str, int]:
    provider = make_provider(account)
    conn = init_db(db_path)  # each thread gets its own connection
    saved = 0

    try:
        with provider:
            if not provider.health_check():
                raise RuntimeError(f"Health check failed for {account.name!r}")

            for folder, msg in provider.fetch_emails(spec):
                progress.increment(folder)
                uid = message_uid(msg)

                insert_message(
                    conn,
                    message_id=uid,
                    account=account.name,
                    folder=folder,
                    date=message_date(msg),
                    subject=msg.get("Subject"),
                    from_addr=extract_addrs(msg, "From"),
                    to_addr=extract_addrs(msg, "To"),
                    cc_addr=extract_addrs(msg, "Cc"),
                    bcc_addr=extract_addrs(msg, "Bcc"),
                    raw=msg.as_bytes(),
                )
                saved += 1

    except KeyboardInterrupt:
        pass
    finally:
        conn.close()

    return account.name, saved


def fetch_all(
    accounts: list[AccountConfig],
    spec: FilterSpec,
    db_path: Path,
    logger: ProgressLogger,
) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    init_db(db_path)  # create tables once in main thread before workers start

    with ThreadPoolExecutor(max_workers=len(accounts)) as pool:
        futures = {
            pool.submit(
                fetch, a, spec, db_path, logger.register(a.name)
            ): a
            for a in accounts
        }
        for future in as_completed(futures):
            account = futures[future]
            try:
                name, saved = future.result()
                log(name, f"Done — {saved} saved")
            except Exception as exc:
                log(account.name, f"ERROR — {exc}")
            finally:
                logger.deregister(account.name)