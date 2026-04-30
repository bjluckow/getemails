from __future__ import annotations

import os
import re
import signal
import sys
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import click
import yaml
from datetime import date, timedelta, timezone


from getemails.account import Account
from getemails.filters import FilterSpec
from getemails.providers.base import AccountConfig, EmailProvider
from getemails.local import filter_local
from getemails.storage import GroupBy, query_folder_name, save_eml
from getemails.logger import ProgressLogger, log

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


def _make_spec(since, until, senders, recipients, cc, bcc, any_addresses=(), use_today=False) -> FilterSpec:
    today = date.today()
    tomorrow = today + timedelta(days=1)
    return FilterSpec(
        senders=list(senders),
        recipients=list(recipients),
        cc=list(cc),
        bcc=list(bcc),
        any_addresses=list(any_addresses),
        since=today if use_today else (since.date() if since else None),
        until=tomorrow if use_today else (until.date() if until else None),
    )


def _run_account(
    config: AccountConfig, spec: FilterSpec,
    query_dir: Path, logger: ProgressLogger,
    group_by: GroupBy | None = None,
) -> tuple[str, int, int]:
    provider = _make_provider(config)
    account = Account.create(config, provider, logger)
    saved, skipped = account.fetch(spec, query_dir, group_by=group_by)
    return config.name, saved, skipped


# --- shared options ----------------------------------------------------------

def _filter_options(f):
    f = click.option("--since", default=None, type=click.DateTime(formats=["%Y-%m-%d"]),
                     help="Only include emails on or after this date.")(f)
    f = click.option("--until", default=None, type=click.DateTime(formats=["%Y-%m-%d"]),
                     help="Only include emails on or before this date.")(f)
    f = click.option("--today", "use_today", is_flag=True, default=False,
                 help="Only include emails from today (local timezone). Cannot be used with --since or --until.")(f)
    f = click.option("--sender", "senders", multiple=True,
                     help="Filter by sender address (repeatable).")(f)
    f = click.option("--recipient", "recipients", multiple=True,
                     help="Filter by recipient address (repeatable).")(f)
    f = click.option("--cc", "cc", multiple=True,
                     help="Filter by CC address (repeatable).")(f)
    f = click.option("--bcc", "bcc", multiple=True,
                     help="Filter by BCC address (repeatable).")(f)
    f = click.option("--any-address", "any_addresses", multiple=True,
                 help="Match address in any field: from, to, cc, or bcc (repeatable).")(f)
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
@click.option("--log-interval", default=30, show_default=True,
              help="Seconds between progress updates.")
@click.option("--output", "output_name", default=None,
              help="Output directory name (overrides auto-generated name).")
@click.option("--group-by-date", is_flag=True, default=False,
              help="Group emails into subdirectories by date (YYYY-MM-DD).")
@click.option("--group-by-thread", is_flag=True, default=False,
              help="Group emails into subdirectories by thread subject.")
@_filter_options
def fetch(config, account_name, log_interval, output_name, 
          group_by_date, group_by_thread, since, until, use_today, 
          senders, recipients, cc, bcc, any_addresses):
    if use_today and (since or until):
        raise click.UsageError("--today cannot be combined with --since or --until.")
    
    """Download emails from configured accounts to .eml files."""
    accounts = _load_accounts(Path(config))

    if account_name:
        accounts = [a for a in accounts if a.name == account_name]
        if not accounts:
            raise click.ClickException(f"No account named {account_name!r} in config.")

    spec = _make_spec(since, until, senders, recipients, cc, bcc, any_addresses, use_today=use_today)

    click.echo(f"Fetching {len(accounts)} account(s) in parallel...\n")
    group_by = GroupBy(date=group_by_date, thread=group_by_thread)
    query_dir = OUTPUT_DIR / (output_name if output_name else query_folder_name(spec))
    click.echo(f"Output directory: {query_dir}\n")


    progress_logger = ProgressLogger(interval=log_interval)

    def _handle_interrupt(sig, frame):
        log("getemails", "Interrupted — saving progress and exiting...")
        progress_logger.stop()
        sys.exit(0)
    signal.signal(signal.SIGINT, _handle_interrupt)

    progress_logger.start()

    with ThreadPoolExecutor(max_workers=len(accounts)) as pool:
        futures = {
            pool.submit(_run_account, a, spec, query_dir, progress_logger, group_by): a
            for a in accounts
        }
        for future in as_completed(futures):
            account = futures[future]
            try:
                name, saved, skipped = future.result()
                log(name, f"Done — {saved} saved, {skipped} skipped")
            except Exception as exc:
                log(account.name, f"ERROR — {exc}")

    progress_logger.stop()

    click.echo("\nDone.")


@cli.command()
@click.argument("input_path", type=click.Path(exists=True))
@click.option("--mbox", "use_mbox", is_flag=True, default=False,
              help="Treat INPUT_PATH as an .mbox file instead of a directory.")
@click.option("--recursive/--no-recursive", default=False, show_default=True,
              help="Walk input directory recursively (ignored with --mbox).")
@click.option("--output", "output_dir", default=None, type=click.Path(),
              help="Output directory (default: output/<query>).")
@click.option("--group-by-date", is_flag=True, default=False,
              help="Group emails into subdirectories by date (YYYY-MM-DD).")
@click.option("--group-by-thread", is_flag=True, default=False,
              help="Group emails into subdirectories by thread subject.")
@_filter_options
def local(input_path, use_mbox, recursive, output_dir, group_by_date, group_by_thread, 
          since, until, use_today, senders, recipients, cc, bcc, any_addresses):
    """Filter .eml files or an .mbox file into a new output directory.

    INPUT_PATH is either a directory of .eml files or an .mbox file (with --mbox).
    """
    if use_today and (since or until):
        raise click.UsageError("--today cannot be combined with --since or --until.")
    
    spec = _make_spec(since, until, senders, recipients, cc, bcc, any_addresses, use_today=use_today)
    out_dir = Path(output_dir) if output_dir else OUTPUT_DIR / query_folder_name(spec)
    click.echo(f"Output directory: {out_dir}\n")

    saved, skipped, filtered = filter_local(
        Path(input_path), out_dir, spec, recursive=recursive, mbox=use_mbox, group_by=GroupBy(date=group_by_date, thread=group_by_thread)
    )
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
                from getemails.providers.gmail import GmailProvider
                if isinstance(provider, IMAPProvider):
                    for folder in provider._list_folders():
                        click.echo(f"  {folder}")
                elif isinstance(provider, GmailProvider):
                    for label in provider.list_labels():
                        click.echo(f"  {label}")
                else:
                    click.echo("  (folder listing not supported for this provider)")
        except Exception as exc:
            click.echo(f"  ERROR — {exc}", err=True)



def main() -> None:
    cli()