from __future__ import annotations

import re
import signal
import sys
from datetime import date, timedelta
from pathlib import Path

import click

from getemails.config import Config, DEFAULT_CONFIG_PATH
from getemails.db import init_db, insert_from_stream, get_stats, delete_messages
from getemails.email_utils import stream_emls, stream_mbox
from getemails.fetch import fetch_all, fetch_folders
from getemails.filters import FilterSpec
from getemails.writer import write_emls
from getemails.logger import ProgressLogger, log
from getemails.sorting import SortingSpec


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

def _build_folder_name(spec: FilterSpec) -> str:
    """Build a human-readable output folder name from a FilterSpec."""
    parts: list[str] = []

    if spec.since or spec.until:
        since = spec.since.strftime("%Y-%m-%d") if spec.since else "oldest"
        until = spec.until.strftime("%Y-%m-%d") if spec.until else date.today().strftime("%Y-%m-%d")
        parts.append(f"{since}_{until}")

    if spec.senders:
        parts.append("from-" + "+".join(spec.senders))
    if spec.recipients:
        parts.append("to-" + "+".join(spec.recipients))
    if spec.cc:
        parts.append("cc-" + "+".join(spec.cc))
    if spec.bcc:
        parts.append("bcc-" + "+".join(spec.bcc))
    if spec.any_addresses:
        parts.append("any-" + "+".join(spec.any_addresses))

    raw = "__".join(parts) if parts else "all"
    return re.sub(r"[^\w@.+_-]", "-", raw)


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
    f = click.option("--any", "any_addresses", multiple=True,
                     help="Match address in any field: from, to, cc, or bcc (repeatable).")(f)
    return f


def _sorting_options(f):
    f = click.option("--group-by-date", is_flag=True, default=False,
                     help="Group emails into subdirectories by date (YYYY-MM-DD).")(f)
    f = click.option("--group-by-folder", is_flag=True, default=False,
                     help="Group emails into subdirectories by source folder.")(f)
    f = click.option("--group-by-thread", is_flag=True, default=False, 
                     help="Group emails into subdirectories by thread subject.")(f)
    return f


# --- commands ----------------------------------------------------------------

@click.group()
def cli() -> None:
    pass


@cli.command(name="fetch")
@click.option("--config", "config_path", default="config/accounts.yaml", show_default=True,
              type=click.Path(exists=True), help="Path to accounts config.")
@click.option("--account", "account_names", multiple=True,
              help="Account name to fetch (repeatable). Defaults to all accounts.")
@click.option("--log-interval", default=30, show_default=True,
              help="Seconds between progress updates.")
@_filter_options
def fetch(config_path, account_names, log_interval,
          since, until, use_today, senders, recipients, cc, bcc, any_addresses):
    """Download emails from configured accounts to .eml files."""
    if use_today and (since or until):
        raise click.UsageError("--today cannot be combined with --since or --until.")

    cfg = Config.load(config_path=config_path)

    if account_names:
        accounts = [a for a in cfg.accounts if a.name in account_names]
        missing = set(account_names) - {a.name for a in accounts}
        if missing:
            raise click.ClickException(f"No account(s) named {', '.join(missing)!r} in config.")
    else:
        accounts = cfg.accounts

    spec = _make_spec(since, until, senders, recipients, cc, bcc, any_addresses, use_today=use_today)

    click.echo(f"Fetching {len(accounts)} account(s) in parallel...\n")
    click.echo(f"Database: {cfg.db_path.resolve()}\n")

    progress_logger = ProgressLogger(interval=log_interval)

    def _handle_interrupt(sig, frame):
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        log("getemails", "Interrupted — saving progress and exiting...")
        progress_logger.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _handle_interrupt)
    progress_logger.start()
    fetch_all(accounts, spec, db_path=cfg.db_path, logger=progress_logger)
    progress_logger.stop()

    click.echo("\nDone.")



@cli.command(name="folders")
@click.option("--config", "config_path", default="config/accounts.yaml", show_default=True,
              type=click.Path(exists=True), help="Path to accounts config.")
@click.option("--account", "account_names", multiple=True, help="Show folders for a single account by name.")
def folders(config_path, account_names):
    """List all folders for configured accounts."""
    cfg = Config.load(config_path=config_path)

    if account_names:
        accounts = [a for a in cfg.accounts if a.name in account_names]
        missing = set(account_names) - {a.name for a in accounts}
        if missing:
            raise click.ClickException(f"No account(s) named {', '.join(missing)!r} in config.")
    else:
        accounts = cfg.accounts

    for account in accounts:
        click.echo(f"\n{account.name}:")
        try:
            for folder in fetch_folders(account):
                click.echo(f"  {folder}")
        except Exception as exc:
            click.echo(f"  ERROR — {exc}", err=True)


@cli.command(name="local")
@click.argument("input_path", type=click.Path(exists=True), required=False, default=None)
@click.option("--config", "config_path", default=str(DEFAULT_CONFIG_PATH), show_default=True,
              type=click.Path(exists=True), help="Path to accounts config.")
@click.option("--output", "output_dir", default=None, type=click.Path(),
              help="Output directory (default: output/<query>).")
@_sorting_options
@_filter_options
def local(input_path, config_path, output_dir, 
          group_by_date, group_by_folder,group_by_thread,
          since, until, use_today, senders, recipients, cc, bcc, any_addresses):
    """Filter .eml files or an .mbox file into a new output directory.

    INPUT_PATH is either a directory of .eml files or an .mbox file (with --mbox).
    """
    if use_today and (since or until):
        raise click.UsageError("--today cannot be combined with --since or --until.")
    
    cfg = Config.load(config_path=config_path)

    # only use input_path if explicitly provided (e.g. for mbox)
    resolved_input = Path(input_path) if input_path else cfg.db_path.parent

    filter_spec = _make_spec(since, until, senders, recipients, cc, bcc, any_addresses, use_today=use_today)
    out_dir = Path(output_dir) if output_dir else cfg.output_dir / _build_folder_name(filter_spec)
    sorting_spec = SortingSpec(
        groupby_date=group_by_date,
        groupby_folder=group_by_folder,
        groupby_thread=group_by_thread,
    )

    click.echo(f"Output directory: {out_dir}\n")

    saved, skipped = write_emls(resolved_input, out_dir, 
        filter_spec=filter_spec, sorting_spec=sorting_spec, 
    )
    click.echo(f"  {saved} saved, {skipped} skipped")
    click.echo("\nDone.")


@cli.command(name="import")
@click.argument("input_path", type=click.Path(exists=True))
@click.argument("account")
@click.option("--config", "config_path", default=str(DEFAULT_CONFIG_PATH), show_default=True,
              type=click.Path(exists=True), help="Path to accounts config.")
@click.option("--folder", "import_folder", default="inbox",
              help="Folder label for imported messages.")
@click.option("--recursive/--no-recursive", default=False, show_default=True,
              help="Walk INPUT_PATH recursively when importing a directory.")
@_filter_options
def import_cmd(input_path, account, config_path, import_folder, recursive,
               since, until, use_today, senders, recipients, cc, bcc, any_addresses):
    """Import an .mbox file or directory of .eml files into the database.

    ACCOUNT is the email address to associate with imported messages.
    INPUT_PATH is either an .mbox file or a directory of .eml files.
    """
    if use_today and (since or until):
        raise click.UsageError("--today cannot be combined with --since or --until.")

    cfg = Config.load(config_path=config_path)
    spec = _make_spec(since, until, senders, recipients, cc, bcc, any_addresses, use_today=use_today)
    path = Path(input_path)
    source = stream_mbox(path) if path.is_file() else stream_emls(path, recursive=recursive)

    conn = init_db(cfg.db_path)
    inserted, filtered = insert_from_stream(conn, source, account, import_folder, spec)
    conn.close()

    click.echo(f"  {inserted} imported, {filtered} filtered out")
    click.echo(f"  Database: {cfg.db_path.resolve()}")
    click.echo("\nImport complete.")


@cli.command(name="stats")
@click.option("--config", "config_path", default=str(DEFAULT_CONFIG_PATH), show_default=True,
              type=click.Path(exists=True), help="Path to accounts config.")
def stats(config_path):
    """Show database stats and email counts per account and folder."""
    cfg = Config.load(config_path=config_path)

    click.echo(f"Config:   {cfg.cfg_path.resolve()}")
    click.echo(f"Database: {cfg.db_path.resolve()}")

    if not cfg.db_path.exists():
        click.echo("\nNo database found — run `getemails fetch` first.")
        return

    conn = init_db(cfg.db_path)
    account_stats = get_stats(conn)
    conn.close()

    if not account_stats:
        click.echo("\nDatabase is empty.")
        return

    header = f"\n{'Address':<40} {'Emails':>8}  {'Earliest':<12}  {'Latest':<12}"
    click.echo(header)
    click.echo("-" * 78)

    total = 0
    for stat in account_stats:
        click.echo(
            f"{stat.account:<40} {stat.count:>8}  "
            f"{stat.earliest or '?':<12}  {stat.latest or '?':<12}"
        )
        for f in stat.folders:
            label = "  " + f.folder
            click.echo(
                f"{label:<40} {f.count:>8}  "
                f"{f.earliest or '?':<12}  {f.latest or '?':<12}"
            )
        total += stat.count

    click.echo("-" * 78)
    click.echo(f"{'Total':<40} {total:>8}")

@cli.command()
@click.option("--config", "config_path", default=str(DEFAULT_CONFIG_PATH), show_default=True,
              type=click.Path(exists=True), help="Path to accounts config.")
@click.option("--yes", is_flag=True, default=False,
              help="Skip confirmation prompt.")
@_filter_options
def clean(config_path, yes, since, until, use_today, senders, recipients, cc, bcc, any_addresses):
    """Remove emails from the database matching the given filters."""
    if use_today and (since or until):
        raise click.UsageError("--today cannot be combined with --since or --until.")

    cfg = Config.load(config_path=config_path)

    if not cfg.db_path.exists():
        click.echo("No database found.")
        return

    spec = _make_spec(since, until, senders, recipients, cc, bcc, any_addresses, use_today=use_today)

    if spec.is_empty() and not yes:
        raise click.UsageError(
            "No filters provided — this will delete all messages. Pass --yes to confirm."
        )

    if not yes:
        click.confirm(
            f"Delete messages matching filters from {cfg.db_path.resolve()}?",
            abort=True,
        )

    conn = init_db(cfg.db_path)
    deleted = delete_messages(conn, spec if not spec.is_empty() else None)
    conn.close()

    click.echo(f"  {deleted} message(s) deleted.")


def main() -> None:
    cli()