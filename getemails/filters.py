from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date


@dataclass
class FilterSpec:
    senders: list[str] = field(default_factory=list)
    recipients: list[str] = field(default_factory=list)
    since: date | None = None
    until: date | None = None

    def is_empty(self) -> bool:
        return not any([self.senders, self.recipients, self.since, self.until])