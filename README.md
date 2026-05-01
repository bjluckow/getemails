# `emlar` - **em**ai**l** **ar**chiver

A command-line tool for importing, searching, and exporting email archives.

Stores email in a local SQLite database. Import from `.mbox` or `.eml` files, filter by date and address, export to `.eml`.

## Installation

```bash
pip install git+https://github.com/bjluckow/emlar.git
```

## Usage

```bash
emlar --help
emlar import --help
emlar export --help
```

## Getting your email

- **Gmail** — [Google Takeout](https://takeout.google.com), select Gmail, export as `.mbox`
- **iCloud** — [privacy.apple.com](https://privacy.apple.com), select Mail
- **AOL/Yahoo** — [yahoo-mail-dl](https://github.com/bjluckow/yahoo-mail-dl)

## Database

Email is stored as raw RFC 2822 bytes in `~/.emlar/emails.db` by default. Exported `.eml` and `.mbox` files include an `X-Folder` header preserving the source folder. Re-importing the same message is safe — duplicates are silently ignored.