from __future__ import annotations

import email
import email.policy
from datetime import date
from email.message import EmailMessage
from typing import Generator, Iterator

from imapclient import IMAPClient

from emlar.config import AccountConfig
from emlar.filters import FilterSpec
from emlar.providers.base import EmailProvider

UID_BATCH_SIZE = 100

# iCloud requires BODY[] instead of RFC822 — we detect this per-server
# by attempting RFC822 on the first fetch and falling back if needed.
FETCH_KEY_RFC822 = b"RFC822"
FETCH_KEY_BODY = b"BODY[]"


class IMAPProvider(EmailProvider):
    """
    Shared IMAP logic for iCloud and AOL.
    Subclasses must set HOST and PORT.
    """

    HOST: str
    PORT: int = 993

    def __init__(self, account: AccountConfig) -> None:
        super().__init__(account)
        self._client: IMAPClient | None = None
        self._fetch_key: bytes = FETCH_KEY_RFC822

    def connect(self) -> None:
        username = self.account.username
        password = self.account.password
        if not username or not password:
            raise ValueError(f"Account {self.account.name!r} is missing username or password.")
        self._client = IMAPClient(self.HOST, port=self.PORT, ssl=True, use_uid=True)
        self._client.login(username, password)

    def disconnect(self) -> None:
        if self._client:
            try:
                self._client.logout()
            except Exception:
                pass
            self._client = None

    def health_check(self) -> bool:
        if not self._client:
            return False
        try:
            self._client.noop()
            return True
        except Exception:
            return False
        
    def get_address(self) -> str:
        assert self.account.username
        return self.account.username

    def fetch_emails(self, spec: FilterSpec) -> Iterator[tuple[str, EmailMessage]]:
        assert self._client, "Not connected — call connect() first"

        folders = self._list_folders()
        criteria = build_imap_criteria(spec)
        seen_mids: set[str] = set()

        for folder in folders:
            folder_info = self._client.select_folder(folder, readonly=True)
            if not int(folder_info.get(b"EXISTS", 0)):
                continue

            uids: list[int] = self._client.search(criteria)  # type: ignore[arg-type]
            if not uids:
                continue

            for batch in uid_batches(uids, UID_BATCH_SIZE):
                for msg in self._fetch_batch(batch):
                    mid = msg.get("Message-ID", "")
                    if mid and mid in seen_mids:
                        continue
                    if mid:
                        seen_mids.add(mid)
                    yield folder, msg

    def _fetch_batch(self, uids: list[int]) -> list[EmailMessage]:
        """
        Fetch a batch of UIDs as EmailMessage objects.
        Tries RFC822 first; if the server returns no body data (e.g. iCloud),
        falls back to BODY[] and remembers that for subsequent fetches.
        """
        assert self._client
        messages = []

        response = self._client.fetch(uids, [self._fetch_key])
        raw_values = [
            data.get(self._fetch_key)
            for data in response.values()
        ]

        # If every value is None the server ignored our fetch key — fall back
        if all(v is None for v in raw_values) and self._fetch_key == FETCH_KEY_RFC822:
            self._fetch_key = FETCH_KEY_BODY
            response = self._client.fetch(uids, [self._fetch_key])

        for data in response.values():
            raw = data.get(self._fetch_key)
            if not isinstance(raw, bytes):
                continue
            msg = email.message_from_bytes(raw, policy=email.policy.default)
            messages.append(msg)

        return messages

    def _list_folders(self) -> list[str]:
        assert self._client
        if self.account.folders:
            return self.account.folders
        folders = []
        for flags, _delimiter, name in self._client.list_folders():
            if isinstance(name, bytes):
                name = name.decode()
            if b"\\Noselect" in flags:
                continue
            folders.append(name)
        return folders


def uid_batches(uids: list[int], size: int) -> Generator[list[int], None, None]:
    for i in range(0, len(uids), size):
        yield uids[i : i + size]


def build_imap_criteria(spec: FilterSpec) -> list[object]:
    criteria: list[object] = []

    if spec.since:
        criteria += ["SINCE", spec.since]
    if spec.until:
        criteria += ["BEFORE", spec.until]
    if spec.senders:
        criteria += _or_criteria("FROM", spec.senders)
    if spec.recipients:
        criteria += _or_criteria("TO", spec.recipients)
    if spec.cc:
        criteria += _or_criteria("CC", spec.cc)
    if spec.bcc:
        criteria += _or_criteria("BCC", spec.bcc)
    if spec.any_addresses:
        for addr in spec.any_addresses:
            criteria.append(
                ["OR", ["FROM", addr],
                ["OR", ["TO", addr],
                ["OR", ["CC", addr], ["BCC", addr]]]]
            )

    return criteria if criteria else ["ALL"]


def _or_criteria(field: str, values: list[str]) -> list[object]:
    if len(values) == 1:
        return [field, values[0]]
    result: list[object] = [field, values[-1]]
    for value in reversed(values[:-1]):
        result = ["OR", [field, value], result]
    return result