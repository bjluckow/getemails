from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml
from dotenv import load_dotenv

load_dotenv()

OUTPUT_DIR = Path("output")
DEFAULT_CONFIG_PATH = Path("config/accounts.yaml")


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


@dataclass
class Config:
    accounts: list[AccountConfig]
    cfg_path: Path
    db_path: Path
    output_dir: Path = field(default_factory=lambda: OUTPUT_DIR)

    @classmethod
    def load(
        cls,
        config_path: Path | str = DEFAULT_CONFIG_PATH,
        db_name: str = "emails.db",
        output_name: str | None = None,
    ) -> Config:
        config_path = Path(config_path)
        raw = config_path.read_text()
        raw = re.sub(
            r"\$\{(\w+)\}",
            lambda m: os.environ.get(m.group(1), ""),
            raw,
        )
        data = yaml.safe_load(raw)
        accounts = [AccountConfig(**a) for a in data["accounts"]]
        output_dir = OUTPUT_DIR / (output_name or "default")
        db_path = Path(data.get("db_path", output_dir / db_name)).expanduser()
        return cls(
            accounts=accounts,
            cfg_path=config_path,
            db_path=db_path,
            output_dir=output_dir,
        )