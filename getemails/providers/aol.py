from __future__ import annotations

import email
import email.policy
from datetime import date, timedelta
from email.message import EmailMessage
from typing import Iterator

from getemails.filters import FilterSpec
from getemails.providers.imap import IMAPProvider, build_imap_criteria, uid_batches

# AOL/Yahoo enforce MESSAGELIMIT=500 per SEARCH command.
# We split the date range into windows small enough that each window's
# results stay comfortably under that cap.
AOL_WINDOW_DAYS = 30
AOL_FETCH_BATCH = 500

# Folders to skip when archiving — deleted mail and server internals.
# Adjust in accounts.yaml via the `folders` key to override entirely.


class AOLProvider(IMAPProvider):
    HOST = "imap.aol.com"
    PORT = 993

    def fetch_emails(self, spec: FilterSpec) -> Iterator[tuple[str, EmailMessage]]:
        assert self._client, "Not connected — call connect() first"

        folders = self._list_folders()
        seen_mids: set[str] = set()

        windows = _date_windows(
            since=spec.since or date(2000, 1, 1),
            until=spec.until or date.today(),
            window_days=AOL_WINDOW_DAYS,
        )

        for folder in folders:
            folder_info = self._client.select_folder(folder, readonly=True)
            if not int(folder_info.get(b"EXISTS", 0)):
                continue

            folder_uids: list[int] = []
            for window_since, window_until in windows:
                window_spec = FilterSpec(
                    senders=spec.senders,
                    recipients=spec.recipients,
                    since=window_since,
                    until=window_until,
                )
                criteria = build_imap_criteria(window_spec)
                uids: list[int] = self._client.search(criteria)  # type: ignore[arg-type]
                folder_uids.extend(uids)

            if not folder_uids:
                continue

            for batch in uid_batches(folder_uids, AOL_FETCH_BATCH):
                for msg in self._fetch_batch(batch):
                    mid = msg.get("Message-ID", "")
                    if mid and mid in seen_mids:
                        continue
                    if mid:
                        seen_mids.add(mid)
                    yield folder, msg


def _date_windows(
    since: date, until: date, window_days: int
) -> list[tuple[date, date]]:
    """Split a date range into fixed-size windows."""
    windows = []
    start = since
    while start < until:
        end = min(start + timedelta(days=window_days), until)
        windows.append((start, end))
        start = end
    return windows