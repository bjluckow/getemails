from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field

from rich.console import Console
from rich.text import Text

console = Console(highlight=False)

# Docker-compose style colors — assigned round-robin per account
ACCOUNT_COLORS = [
    "cyan", "green", "magenta", "yellow", "blue",
    "bright_red", "bright_cyan", "bright_green",
]

_color_lock = threading.Lock()
_color_index = 0
_account_colors: dict[str, str] = {}


def get_account_color(account_name: str) -> str:
    global _color_index
    with _color_lock:
        if account_name not in _account_colors:
            _account_colors[account_name] = ACCOUNT_COLORS[_color_index % len(ACCOUNT_COLORS)]
            _color_index += 1
        return _account_colors[account_name]


def log(account_name: str, message: str) -> None:
    color = get_account_color(account_name)
    prefix = Text(f"[{account_name}] ", style=f"bold {color}")
    text = Text(message)
    console.print(prefix + text)


@dataclass
class FolderProgress:
    name: str
    done: int = 0

@dataclass
class AccountProgress:
    account_name: str
    done: int = 0
    folders: dict[str, FolderProgress] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def increment(self, folder: str, n: int = 1) -> None:
        with self._lock:
            self.done += n
            if folder not in self.folders:
                self.folders[folder] = FolderProgress(name=folder)
            self.folders[folder].done += n

    def print_status(self) -> None:
        color = get_account_color(self.account_name)
        prefix = f"[bold {color}][{self.account_name}][/bold {color}]"
        lines = [f"{prefix} Total: {self.done} processed"]
        for fp in self.folders.values():
            if fp.done > 0:
                lines.append(f"{prefix} Folder ({fp.name}): {fp.done} processed")
        console.print("\n".join(lines))


class ProgressLogger:
    """
    Periodically prints status for all registered accounts.
    Each account registers itself and updates its own progress.
    A background thread fires every `interval` seconds.
    """

    def __init__(self, interval: int = 30) -> None:
        self.interval = interval
        self._accounts: dict[str, AccountProgress] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def register(self, account_name: str) -> AccountProgress:
        progress = AccountProgress(account_name=account_name)
        with self._lock:
            self._accounts[account_name] = progress
        return progress

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join()
        self._print_all()  # final summary

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            self._print_all()

    def _print_all(self) -> None:
        with self._lock:
            accounts = list(self._accounts.values())
        for progress in accounts:
            progress.print_status()