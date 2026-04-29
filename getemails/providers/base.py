from __future__ import annotations

import email.policy
from abc import ABC, abstractmethod
from typing import Iterator
from dataclasses import dataclass, field
from email.message import EmailMessage

from getemails.filters import FilterSpec


@dataclass
class AccountConfig:
    name: str
    provider: str
    # Gmail
    credentials_file: str | None = None
    token_file: str | None = None
    # IMAP (iCloud, AOL)
    username: str | None = None
    password: str | None = None
    folders: list[str] | None = None  # if set, only search these folders


class EmailProvider(ABC):
    def __init__(self, account: AccountConfig) -> None:
        self.account = account

    @abstractmethod
    def connect(self) -> None:
        """Authenticate and open a connection to the mail server."""

    @abstractmethod
    def disconnect(self) -> None:
        """Close the connection cleanly."""

    @abstractmethod
    def fetch_emails(self, spec: FilterSpec) -> Iterator[tuple[str, EmailMessage]]:
        """Yield (folder, msg) tuples matching spec one at a time.
        folder is a human-readable label e.g. 'Inbox', 'Bulk', 'Gmail'.
        """

    @abstractmethod
    def health_check(self) -> bool:
        """Return True if the connection is alive and usable."""

    def __enter__(self) -> EmailProvider:
        self.connect()
        return self

    def __exit__(self, *_) -> None:
        self.disconnect()