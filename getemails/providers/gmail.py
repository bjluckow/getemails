from __future__ import annotations

import base64
import email
import email.policy
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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

    def list_labels(self) -> list[str]:
        """Return all label names for this account — used by `getemails folders`."""
        assert self._service, "Not connected — call connect() first"
        result = self._service.users().labels().list(userId="me").execute()
        return sorted(label["name"] for label in result.get("labels", []))

    def fetch_emails(self, spec: FilterSpec) -> list[EmailMessage]:
        assert self._service, "Not connected — call connect() first"

        query = _build_gmail_query(spec)
        print(f"  {self.account.name}: listing messages...")
        msg_ids = _list_all_message_ids(self._service, query)
        total = len(msg_ids)
        print(f"  {self.account.name}: {total} messages found, fetching...")

        messages: list[EmailMessage] = []
        done = 0
        batches = list(_batched(msg_ids, 100))
        creds = self._service._http.credentials

        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = {
                pool.submit(_fetch_batch_with_backoff, creds, b): b
                for b in batches
            }
            for future in as_completed(futures):
                messages.extend(future.result())
                done += len(futures[future])
                print(f"\r  {self.account.name}: {done}/{total}   ", end="", flush=True)

        if total:
            print()

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
    if spec.cc:
        cc_clause = " OR ".join(f"cc:{c}" for c in spec.cc)
        parts.append(f"({cc_clause})" if len(spec.cc) > 1 else cc_clause)
    if spec.bcc:
        bcc_clause = " OR ".join(f"bcc:{b}" for b in spec.bcc)
        parts.append(f"({bcc_clause})" if len(spec.bcc) > 1 else bcc_clause)

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


def _fetch_batch(service, msg_ids: list[str]) -> list[EmailMessage]:
    """Fetch up to 100 messages in a single HTTP batch request."""
    results: list[EmailMessage] = []

    def callback(request_id: str, response: dict, exception: Exception | None) -> None:
        if exception or not response:
            return
        raw_b64 = response.get("raw")
        if not raw_b64:
            return
        raw = base64.urlsafe_b64decode(raw_b64 + "==")
        msg = email.message_from_bytes(raw, policy=email.policy.default)
        results.append(msg)

    batch = service.new_batch_http_request(callback=callback)
    for msg_id in msg_ids:
        batch.add(service.users().messages().get(
            userId="me", id=msg_id, format="raw"
        ))
    batch.execute()

    return results


def _fetch_batch_with_backoff(creds, msg_ids: list[str]) -> list[EmailMessage]:
    """Build a thread-local service and fetch a batch with exponential backoff."""
    service = build("gmail", "v1", credentials=creds)
    delay = 1.0
    for attempt in range(5):
        try:
            return _fetch_batch(service, msg_ids)
        except HttpError as e:
            if e.resp.status in (429, 500, 503) and attempt < 4:
                time.sleep(delay)
                delay *= 2
            else:
                raise
    return []


def _batched(items: list[str], size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]