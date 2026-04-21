# getemails

Download emails from Gmail, iCloud, and AOL accounts to local `.eml` files.
Supports filtering by sender, recipient, and date range. Fetches all accounts in parallel.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

Copy `.env` and fill in your app passwords:

```bash
cp .env .env.local   # or just edit .env directly — it's gitignored
```

Copy and edit the accounts config:

```bash
cp config/accounts.example.yaml config/accounts.yaml
```

### Gmail — OAuth2

1. Create a project in [Google Cloud Console](https://console.cloud.google.com)
2. Enable the **Gmail API**
3. Create an OAuth 2.0 **Desktop** client credential and download `credentials.json`
4. Place it at `config/credentials.json`
5. On first run, a browser window will open to authorize access — `token_*.json` is saved automatically

### iCloud & AOL — app passwords

- **iCloud**: [appleid.apple.com](https://appleid.apple.com) → Sign-In and Security → App-Specific Passwords
- **AOL**: [login.aol.com](https://login.aol.com) → Account Security → Generate app password

## Usage

```bash
# download all accounts, no filter
getemails

# filter by date range
getemails --since 2024-01-01 --until 2024-03-31

# filter by sender or recipient
getemails --sender boss@company.com --recipient me@gmail.com

# combine filters
getemails --since 2024-01-01 --sender invoices@stripe.com

# target a single account by name
getemails --account work-gmail
```

Output is written to `output/<account-name>/` as `.eml` files:

```
output/
  work-gmail/
    2024-03-15_143022__abc123__Re- project update.eml
  personal-icloud/
    ...
```

Each `.eml` is a self-contained RFC 2822 file — open it in any mail client (Apple Mail, Thunderbird, Outlook). Attachments are preserved inline. Re-running skips already-downloaded messages`.