from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from getemails.filters import FilterSpec
from getemails.logger import AccountProgress, ProgressLogger, log
from getemails.providers.base import AccountConfig, EmailProvider
from getemails.storage import save_eml


@dataclass
class Account:
    config: AccountConfig
    provider: EmailProvider
    progress: AccountProgress

    @classmethod
    def create(cls, config: AccountConfig, provider: EmailProvider, logger: ProgressLogger) -> Account:
        progress = logger.register(config.name)
        return cls(config=config, provider=provider, progress=progress)

    def fetch(self, spec: FilterSpec, output_dir: Path) -> tuple[int, int]:
        saved = skipped = 0
        out_dir = output_dir / self.config.name

        with self.provider:
            if not self.provider.health_check():
                raise RuntimeError(f"Health check failed for {self.config.name!r}")

            for folder, msg in self.provider.fetch_emails(spec):
                self.progress.increment(folder)
                path = save_eml(msg, out_dir)
                if path:
                    saved += 1
                else:
                    skipped += 1

        return saved, skipped