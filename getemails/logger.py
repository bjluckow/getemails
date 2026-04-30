from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime

from rich.console import Console
from rich.text import Text

console = Console(highlight=False, force_terminal=True)

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


def _make_line(account_name: str, message: str) -> Text:
    color = get_account_color(account_name)
    ts = datetime.now().strftime("%H:%M:%S")
    t = Text()
    t.append(f"[{account_name}]", style=f"bold {color}")
    t.append(f" [{ts}] {message}")
    return t


def log(account_name: str, message: str) -> None:
    console.print(_make_line(account_name, message))


@dataclass
class FolderProgress:
    name: str
    done: int = 0
    _last_reported: int = field(default=0, repr=False)


@dataclass
class AccountProgress:
    account_name: str
    done: int = 0
    _last_reported: int = field(default=0, repr=False)
    folders: dict[str, FolderProgress] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def increment(self, folder: str, n: int = 1) -> None:
        with self._lock:
            self.done += n
            if folder not in self.folders:
                self.folders[folder] = FolderProgress(name=folder)
            self.folders[folder].done += n

    def print_status(self) -> None:
        with self._lock:
            new_this_tick = self.done - self._last_reported
            if new_this_tick <= 0:
                return

            console.print(_make_line(
                self.account_name,
                f"+{new_this_tick} processed (total: {self.done})"
            ))

            for fp in self.folders.values():
                delta = fp.done - fp._last_reported
                if delta > 0:
                    console.print(_make_line(
                        self.account_name,
                        f"Folder ({fp.name}): +{delta} (total: {fp.done})"
                    ))
                    fp._last_reported = fp.done

            self._last_reported = self.done


class ProgressLogger:
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

    def deregister(self, account_name: str) -> None:
        with self._lock:
            self._accounts.pop(account_name, None)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join()

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            self._print_all()

    def _print_all(self) -> None:
        with self._lock:
            accounts = list(self._accounts.values())
        for progress in accounts:
            progress.print_status()