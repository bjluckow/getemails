from __future__ import annotations

import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import click
import yaml
from dotenv import load_dotenv

from getemails.filters import FilterSpec
from getemails.providers.base import AccountConfig, EmailProvider
from getemails.local import filter_local
from getemails.storage import query_folder_name, save_eml

load_dotenv()

OUTPUT_DIR = Path("output")


def _load_accounts(config_path: Path) -> list[AccountConfig]:
    raw = config_path.read_text()
    raw = re.sub(
        r"\$\{(\w+)\}",
        lambda m: os.environ.get(m.group(1), ""),
        raw,
    )
    data = yaml.safe_load(raw)
    return [AccountConfig(**a) for a in data["accounts"]]


def _make_provider(account: AccountConfig) -> EmailProvider:
    match account.provider:
        case "gmail":
            from getemails.providers.gmail import GmailProvider
            return GmailProvider(account)
        case "icloud":
            from getemails.providers.icloud import iCloudProvider
            return iCloudProvider(account)
        case "aol":
            from getemails.providers.aol import AOLProvider
            return AOLProvider(account)
        case _:
            raise ValueError(f"Unknown provider: {account.provider!r}")


def _make_spec(since, until, senders, recipients, cc, bcc) -> FilterSpec:
    return FilterSpec(
        senders=list(senders),
        recipients=list(recipients),
        cc=list(cc),
        bcc=list(bcc),
        since=since.date() if since else None,
        until=until.date() if until else None,
    )


def _run_account(
    account: AccountConfig, spec: FilterSpec, query_dir: Path
) -> tuple[str, int, int]:
    provider = _make_provider(account)
    out_dir = query_dir / account.name
    saved = skipped = 0

    with provider:
        if not provider.health_check():
            raise RuntimeError(f"Health check failed for {account.name!r}")
        for msg in provider.fetch_emails(spec):
            path = save_eml(msg, out_dir)
            if path:
                saved += 1
            else:
                skipped += 1

    return account.name, saved, skipped


# --- shared options ----------------------------------------------------------

def _filter_options(f):
    f = click.option("--since", default=None, type=click.DateTime(formats=["%Y-%m-%d"]),
                     help="Only include emails on or after this date.")(f)
    f = click.option("--until", default=None, type=click.DateTime(formats=["%Y-%m-%d"]),
                     help="Only include emails on or before this date.")(f)
    f = click.option("--sender", "senders", multiple=True,
                     help="Filter by sender address (repeatable).")(f)
    f = click.option("--recipient", "recipients", multiple=True,
                     help="Filter by recipient address (repeatable).")(f)
    f = click.option("--cc", "cc", multiple=True,
                     help="Filter by CC address (repeatable).")(f)
    f = click.option("--bcc", "bcc", multiple=True,
                     help="Filter by BCC address (repeatable).")(f)
    return f


# --- commands ----------------------------------------------------------------

@click.group()
def cli() -> None:
    pass


@cli.command()
@click.option("--config", default="config/accounts.yaml", show_default=True,
              type=click.Path(exists=True), help="Path to accounts config.")
@click.option("--account", "account_name", default=None,
              help="Run a single account by name.")
@_filter_options
def fetch(config, account_name, since, until, senders, recipients, cc, bcc):
    """Download emails from configured accounts to .eml files."""
    accounts = _load_accounts(Path(config))

    if account_name:
        accounts = [a for a in accounts if a.name == account_name]
        if not accounts:
            raise click.ClickException(f"No account named {account_name!r} in config.")

    spec = _make_spec(since, until, senders, recipients, cc, bcc)

    click.echo(f"Fetching {len(accounts)} account(s) in parallel...\n")
    query_dir = OUTPUT_DIR / query_folder_name(spec)
    click.echo(f"Output directory: {query_dir}\n")

    with ThreadPoolExecutor(max_workers=len(accounts)) as pool:
        futures = {pool.submit(_run_account, a, spec, query_dir): a for a in accounts}
        for future in as_completed(futures):
            account = futures[future]
            try:
                name, saved, skipped = future.result()
                click.echo(f"  {name}: {saved} saved, {skipped} skipped")
            except Exception as exc:
                click.echo(f"  {account.name}: ERROR — {exc}", err=True)

    click.echo("\nDone.")


@cli.command()
@click.argument("input_dir", type=click.Path(exists=True, file_okay=False))
@click.option("--recursive/--no-recursive", default=False, show_default=True,
              help="Walk input_dir recursively.")
@click.option("--output", "output_dir", default=None, type=click.Path(),
              help="Output directory (default: output/<query>).")
@_filter_options
def local(input_dir, recursive, output_dir, since, until, senders, recipients, cc, bcc):
    """Filter already-downloaded .eml files from INPUT_DIR into a new output directory."""
    spec = _make_spec(since, until, senders, recipients, cc, bcc)

    out_dir = Path(output_dir) if output_dir else OUTPUT_DIR / query_folder_name(spec)
    click.echo(f"Output directory: {out_dir}\n")

    saved, skipped, filtered = filter_local(Path(input_dir), out_dir, spec, recursive)
    click.echo(f"  {saved} saved, {skipped} skipped, {filtered} filtered out")
    click.echo("\nDone.")


@cli.command()
@click.option("--config", default="config/accounts.yaml", show_default=True,
              type=click.Path(exists=True), help="Path to accounts config.")
@click.option("--account", "account_name", default=None,
              help="Show folders for a single account by name.")
def folders(config, account_name):
    """List all folders for configured accounts."""
    accounts = _load_accounts(Path(config))

    if account_name:
        accounts = [a for a in accounts if a.name == account_name]
        if not accounts:
            raise click.ClickException(f"No account named {account_name!r} in config.")

    for account in accounts:
        provider = _make_provider(account)
        click.echo(f"\n{account.name}:")
        try:
            with provider:
                from getemails.providers.imap import IMAPProvider
                if isinstance(provider, IMAPProvider):
                    for folder in provider._list_folders():
                        click.echo(f"  {folder}")
                else:
                    click.echo("  (folder listing not supported for this provider)")
        except Exception as exc:
            click.echo(f"  ERROR — {exc}", err=True)


def main() -> None:
    cli()