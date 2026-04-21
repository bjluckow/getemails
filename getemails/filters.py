from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from email.message import EmailMessage
from email.utils import parsedate_to_datetime, parseaddr


@dataclass
class FilterSpec:
    senders: list[str] = field(default_factory=list)
    recipients: list[str] = field(default_factory=list)
    cc: list[str] = field(default_factory=list)
    bcc: list[str] = field(default_factory=list)
    since: date | None = None
    until: date | None = None

    def is_empty(self) -> bool:
        return not any([
            self.senders, self.recipients, self.cc, self.bcc,
            self.since, self.until,
        ])

    def matches(self, msg: EmailMessage) -> bool:
        """
        Local filter — apply spec against a parsed EmailMessage.
        Used by the `local` command to filter already-downloaded .eml files.
        IMAP providers apply equivalent filtering server-side.
        """
        if self.since or self.until:
            try:
                msg_date = parsedate_to_datetime(msg.get("Date", "")).date()
                if self.since and msg_date < self.since:
                    return False
                if self.until and msg_date >= self.until:
                    return False
            except Exception:
                return False

        if self.senders and not _any_match(msg.get("From", ""), self.senders):
            return False
        if self.recipients and not _any_match(msg.get("To", ""), self.recipients):
            return False
        if self.cc and not _any_match(msg.get("Cc", ""), self.cc):
            return False
        if self.bcc and not _any_match(msg.get("Bcc", ""), self.bcc):
            return False

        return True


def _any_match(header_value: str, targets: list[str]) -> bool:
    """Return True if any target address appears in a header value."""
    addresses = [
        addr.lower()
        for _, addr in [parseaddr(part.strip()) for part in header_value.split(",")]
        if addr
    ]
    return any(t.lower() in addresses for t in targets)