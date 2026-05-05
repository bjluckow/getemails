from __future__ import annotations

import re
from datetime import date, timedelta
from pathlib import Path

import click

from emlar.db import init_db, insert_from_stream, get_stats, delete_messages
from emlar.email_utils import stream_emls, stream_mbox
from emlar.filters import FilterSpec
from emlar.writer import write_emls, write_mbox
from emlar.sorting import DateGrouping, SortingSpec

DEFAULT_DB_PATH = Path.home() / ".emlar" / "emails.db"

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
    f = click.option("--group-by-date",
                     type=click.Choice(["day", "month", "year"]), default=None,
                     help="Group emails by date granularity: day, month, or year.")(f)
    f = click.option("--group-by-folder", is_flag=True, default=False,
                     help="Group emails into subdirectories by source folder.")(f)
    f = click.option("--group-by-thread", is_flag=True, default=False,
                     help="Group emails into subdirectories by thread subject.")(f)
    return f


# --- commands ----------------------------------------------------------------

@click.group()
def cli() -> None:
    pass


@cli.command(name="import")
@click.argument("input_path", type=click.Path(exists=True))
@click.option("--db", "db_path", default=DEFAULT_DB_PATH, type=click.Path(path_type=Path),
              help="Path to database.")
@click.option("--label", "label", default=None,
              help="Label for imported messages (default: filename stem).")
@click.option("--folder", "import_folder", default="inbox",
              help="Folder label for imported messages.")
@click.option("--recursive/--no-recursive", default=False, show_default=True,
              help="Walk INPUT_PATH recursively when importing a directory.")
@_filter_options
def import_cmd(input_path, db_path, label, import_folder, recursive,
               since, until, use_today, senders, recipients, cc, bcc, any_addresses):
    if use_today and (since or until):
        raise click.UsageError("--today cannot be combined with --since or --until.")

    path = Path(input_path)
    account = label or path.stem
    spec = _make_spec(since, until, senders, recipients, cc, bcc, any_addresses, use_today=use_today)
    source = stream_mbox(path) if path.is_file() else stream_emls(path, recursive=recursive)

    resolved_db = Path(db_path)
    resolved_db.parent.mkdir(parents=True, exist_ok=True)
    conn = init_db(resolved_db)
    inserted, filtered = insert_from_stream(conn, source, account, import_folder, spec)
    conn.close()

    click.echo(f"  {inserted} imported, {filtered} filtered out")
    click.echo(f"  Database: {resolved_db.resolve()}")
    click.echo("\nImport complete.")


@cli.command(name="export")
@click.option("--db", "db_path", default=DEFAULT_DB_PATH, type=click.Path(path_type=Path),
              help="Path to database.")
@click.option("--out", "out_dir", required=True, type=click.Path(path_type=Path),
              help="Output directory.")
@click.option("--mbox", "use_mbox", is_flag=True, default=False,
              help="Export as a single .mbox file instead of individual .eml files.")
@_sorting_options
@_filter_options
def export(db_path, out_dir, use_mbox, 
           group_by_date, group_by_folder, group_by_thread,
           since, until, use_today, senders, recipients, cc, bcc, any_addresses):
    """Write .eml files from the database into an output directory."""
    if use_today and (since or until):
        raise click.UsageError("--today cannot be combined with --since or --until.")
    
    if use_mbox and (group_by_date or group_by_folder or group_by_thread):
        raise click.UsageError("--mbox cannot be combined with --group-by-* options.")

    filter_spec = _make_spec(since, until, senders, recipients, cc, bcc, any_addresses, use_today=use_today)
    sorting_spec = SortingSpec(
        groupby_date=DateGrouping(group_by_date) if group_by_date else None,
        groupby_folder=group_by_folder,
        groupby_thread=group_by_thread,
    )

    click.echo(f"Output directory: {out_dir}\n")

    if use_mbox:
        saved, skipped = write_mbox(db_path, out_dir, filter_spec)
    else:
        saved, skipped = write_emls(db_path, out_dir, filter_spec, sorting_spec)

    click.echo(f"  {saved} saved, {skipped} skipped")
    click.echo("\nExport complete.")


@cli.command(name="stats")
@click.option("--db", "db_path", default=DEFAULT_DB_PATH, type=click.Path(path_type=Path),
              help="Path to database.")
@_filter_options
def stats(db_path, since, until, use_today, senders, recipients, cc, bcc, any_addresses):
    """Show database stats and email counts per account and folder."""
    if use_today and (since or until):
        raise click.UsageError("--today cannot be combined with --since or --until.")

    spec = _make_spec(since, until, senders, recipients, cc, bcc, any_addresses, use_today=use_today)

    click.echo(f"Database: {db_path.resolve()}")

    if not db_path.exists():
        click.echo("\nNo database found -- import emails first.")
        return

    conn = init_db(db_path)
    account_stats = get_stats(conn, spec if not spec.is_empty() else None)
    conn.close()

    if not account_stats:
        click.echo("\nNo results.")
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


@cli.command("clean")
@click.option("--db", "db_path", default=DEFAULT_DB_PATH, type=click.Path(path_type=Path),
              help="Path to database.")
@click.option("--yes", is_flag=True, default=False,
              help="Skip confirmation prompt.")
@_filter_options
def clean(db_path, yes, since, until, use_today, senders, recipients, cc, bcc, any_addresses):
    """Remove emails from the database matching the given filters."""
    if use_today and (since or until):
        raise click.UsageError("--today cannot be combined with --since or --until.")


    if not db_path.exists():
        click.echo("No database found.")
        return

    spec = _make_spec(since, until, senders, recipients, cc, bcc, any_addresses, use_today=use_today)

    if spec.is_empty() and not yes:
        raise click.UsageError(
            "No filters provided — this will delete all messages. Pass --yes to confirm."
        )

    if not yes:
        click.confirm(
            f"Delete messages matching filters from {db_path.resolve()}?",
            abort=True,
        )

    conn = init_db(db_path)
    deleted = delete_messages(conn, spec if not spec.is_empty() else None)
    conn.close()

    click.echo(f"  {deleted} message(s) deleted.")


def main() -> None:
    cli()