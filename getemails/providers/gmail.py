from __future__ import annotations

import base64
import email
import email.policy
from email.message import EmailMessage
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from getemails.filters import FilterSpec
from getemails.providers.base import AccountConfig, EmailProvider

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]


class GmailProvider(EmailProvider):
    def __init__(self, account: AccountConfig) -> None:
        super().__init__(account)
        self._service = None

    def connect(self) -> None:
        creds = _load_credentials(
            credentials_file=self.account.credentials_file,
            token_file=self.account.token_file,
        )
        self._service = build("gmail", "v1", credentials=creds)

    def disconnect(self) -> None:
        self._service = None

    def health_check(self) -> bool:
        if not self._service:
            return False
        try:
            self._service.users().getProfile(userId="me").execute()
            return True
        except Exception:
            return False

    def fetch_emails(self, spec: FilterSpec) -> list[EmailMessage]:
        assert self._service, "Not connected — call connect() first"

        query = _build_gmail_query(spec)
        msg_ids = _list_all_message_ids(self._service, query)

        messages = []
        for msg_id in msg_ids:
            raw = _fetch_raw(self._service, msg_id)
            if raw is None:
                continue
            msg = email.message_from_bytes(raw, policy=email.policy.default)
            messages.append(msg)

        return messages


# --- helpers -----------------------------------------------------------------

def _load_credentials(
    credentials_file: str | None,
    token_file: str | None,
) -> Credentials:
    if not credentials_file:
        raise ValueError("Gmail account requires a credentials_file.")
    if not token_file:
        raise ValueError("Gmail account requires a token_file.")
 
    token_path = Path(token_file)
    creds: Credentials | None = None
 
    if token_path.exists():
        loaded = Credentials.from_authorized_user_file(str(token_path), SCOPES)
        if isinstance(loaded, Credentials):
            creds = loaded
 
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_file, SCOPES)
            new_creds = flow.run_local_server(port=0)
            if not isinstance(new_creds, Credentials):
                raise RuntimeError("OAuth flow did not return valid credentials.")
            creds = new_creds
        token_path.write_text(creds.to_json())
 
    if creds is None:
        raise RuntimeError("Failed to load credentials.")
 
    return creds


def _build_gmail_query(spec: FilterSpec) -> str:
    """
    Translate a FilterSpec into a Gmail search query string.
    https://support.google.com/mail/answer/7190
    """
    parts: list[str] = []

    if spec.since:
        parts.append(f"after:{spec.since.strftime('%Y/%m/%d')}")
    if spec.until:
        parts.append(f"before:{spec.until.strftime('%Y/%m/%d')}")
    if spec.senders:
        from_clause = " OR ".join(f"from:{s}" for s in spec.senders)
        parts.append(f"({from_clause})" if len(spec.senders) > 1 else from_clause)
    if spec.recipients:
        to_clause = " OR ".join(f"to:{r}" for r in spec.recipients)
        parts.append(f"({to_clause})" if len(spec.recipients) > 1 else to_clause)

    return " ".join(parts)


def _list_all_message_ids(service, query: str) -> list[str]:
    """Page through the Gmail API and collect all matching message IDs."""
    ids: list[str] = []
    page_token: str | None = None

    while True:
        kwargs: dict = {"userId": "me", "q": query, "maxResults": 500}
        if page_token:
            kwargs["pageToken"] = page_token

        result = service.users().messages().list(**kwargs).execute()
        ids += [m["id"] for m in result.get("messages", [])]
        page_token = result.get("nextPageToken")
        if not page_token:
            break

    return ids


def _fetch_raw(service, msg_id: str) -> bytes | None:
    """Fetch a single message as raw RFC 2822 bytes."""
    try:
        result = service.users().messages().get(
            userId="me", id=msg_id, format="raw"
        ).execute()
        raw_b64 = result.get("raw")
        if not raw_b64:
            return None
        return base64.urlsafe_b64decode(raw_b64 + "==")
    except HttpError:
        return None