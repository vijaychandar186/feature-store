"""
CLI for the Feature Store.

Commands:
  view  create   register a feature view / schema
  view  list     list all feature views
  view  get      get a feature view
  feature write  write feature values
  feature get    get online features
  feature pit    point-in-time historical query
  materialize    batch materialize offline → online
  schema check   check schema compatibility
  health         system health
"""

import json
import click
import httpx
from datetime import datetime
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

console = Console()
API = "http://localhost:8000"


def _c(ctx: click.Context) -> httpx.Client:
    return httpx.Client(base_url=ctx.obj["api"], timeout=60)


@click.group()
@click.option("--api", default=API)
@click.pass_context
def cli(ctx: click.Context, api: str):
    """ML Feature Store CLI"""
    ctx.ensure_object(dict)
    ctx.obj["api"] = api


# ------------------------------------------------------------------
# Feature view management
# ------------------------------------------------------------------

@cli.group()
def view():
    """Manage feature views."""


@view.command("create")
@click.argument("spec_file", type=click.Path(exists=True))
@click.option("--compat", default="BACKWARD", type=click.Choice(["BACKWARD", "FORWARD", "FULL", "NONE"]))
@click.pass_context
def view_create(ctx: click.Context, spec_file: str, compat: str):
    """Register a feature view from a JSON spec file."""
    spec = json.loads(Path(spec_file).read_text())
    payload = {"spec": spec, "compatibility_mode": compat}
    with _c(ctx) as c:
        r = c.post("/schemas", json=payload)
        r.raise_for_status()
    console.print_json(json.dumps(r.json()))


@view.command("list")
@click.pass_context
def view_list(ctx: click.Context):
    with _c(ctx) as c:
        r = c.get("/feature-views")
        r.raise_for_status()
    views = r.json()
    t = Table(title=f"{len(views)} feature views")
    t.add_column("Name")
    t.add_column("Entity col")
    t.add_column("Schema v")
    t.add_column("Features")
    t.add_column("Updated")
    for v in views:
        t.add_row(
            v["name"],
            v["entity_column"],
            str(v["schema_version"]),
            str(len(v["features"])),
            v["updated_at"][:19],
        )
    console.print(t)


@view.command("get")
@click.argument("name")
@click.pass_context
def view_get(ctx: click.Context, name: str):
    with _c(ctx) as c:
        r = c.get(f"/feature-views/{name}")
        r.raise_for_status()
    console.print_json(json.dumps(r.json()))


# ------------------------------------------------------------------
# Features
# ------------------------------------------------------------------

@cli.group()
def feature():
    """Read/write feature values."""


@feature.command("write")
@click.argument("feature_view")
@click.argument("rows_file", type=click.Path(exists=True))
@click.pass_context
def feature_write(ctx: click.Context, feature_view: str, rows_file: str):
    rows = json.loads(Path(rows_file).read_text())
    payload = {"feature_view": feature_view, "rows": rows}
    with _c(ctx) as c:
        r = c.post("/features/write", json=payload)
        r.raise_for_status()
    console.print(f"[green]Written {r.json()['written']} rows to '{feature_view}'[/green]")


@feature.command("get")
@click.argument("feature_view")
@click.argument("entity_ids", nargs=-1)
@click.pass_context
def feature_get(ctx: click.Context, feature_view: str, entity_ids: tuple):
    payload = {"feature_view": feature_view, "entity_ids": list(entity_ids)}
    with _c(ctx) as c:
        r = c.post("/features/online", json=payload)
        r.raise_for_status()
    data = r.json()
    t = Table(title=f"Online features — {feature_view}")
    all_keys: list[str] = []
    for features in data["results"].values():
        all_keys.extend(k for k in features if k not in all_keys)
    t.add_column("entity_id")
    for k in all_keys:
        t.add_column(k)
    for eid, features in data["results"].items():
        t.add_row(eid, *[str(features.get(k, "")) for k in all_keys])
    if data["missing_entities"]:
        console.print(f"[yellow]Missing: {data['missing_entities']}[/yellow]")
    console.print(t)


@feature.command("pit")
@click.argument("feature_view")
@click.argument("pit_file", type=click.Path(exists=True))
@click.pass_context
def feature_pit(ctx: click.Context, feature_view: str, pit_file: str):
    """Point-in-time historical retrieval. pit_file: [[entity_id, iso_timestamp], ...]"""
    entity_timestamps = json.loads(Path(pit_file).read_text())
    payload = {"feature_view": feature_view, "entity_timestamps": entity_timestamps}
    with _c(ctx) as c:
        r = c.post("/features/historical", json=payload)
        r.raise_for_status()
    console.print_json(json.dumps(r.json()))


# ------------------------------------------------------------------
# Materialization
# ------------------------------------------------------------------

@cli.command()
@click.argument("feature_view")
@click.option("--sync", is_flag=True, help="Wait for materialization to complete")
@click.pass_context
def materialize(ctx: click.Context, feature_view: str, sync: bool):
    """Batch-materialize offline → online for a feature view."""
    endpoint = f"/materialize/{feature_view}/sync" if sync else f"/materialize/{feature_view}"
    with _c(ctx) as c:
        r = c.post(endpoint)
        r.raise_for_status()
    console.print_json(json.dumps(r.json()))


# ------------------------------------------------------------------
# Schema
# ------------------------------------------------------------------

@cli.group()
def schema():
    """Schema registry operations."""


@schema.command("check")
@click.argument("feature_view")
@click.argument("new_spec_file", type=click.Path(exists=True))
@click.pass_context
def schema_check(ctx: click.Context, feature_view: str, new_spec_file: str):
    new_spec = json.loads(Path(new_spec_file).read_text())
    payload = {"feature_view": feature_view, "new_spec": new_spec}
    with _c(ctx) as c:
        r = c.post("/schemas/check-compatibility", json=payload)
        r.raise_for_status()
    data = r.json()
    colour = "green" if data["compatible"] else "red"
    console.print(f"[{colour}]Compatible: {data['compatible']}[/{colour}]")
    for e in data.get("errors", []):
        console.print(f"  [red]ERROR: {e}[/red]")
    for w in data.get("warnings", []):
        console.print(f"  [yellow]WARN: {w}[/yellow]")


@schema.command("versions")
@click.argument("feature_view")
@click.pass_context
def schema_versions(ctx: click.Context, feature_view: str):
    with _c(ctx) as c:
        r = c.get(f"/schemas/{feature_view}/versions/all")
        r.raise_for_status()
    console.print_json(json.dumps(r.json()))


# ------------------------------------------------------------------
# Health
# ------------------------------------------------------------------

@cli.command()
@click.pass_context
def health(ctx: click.Context):
    with _c(ctx) as c:
        r = c.get("/health")
        r.raise_for_status()
    console.print_json(json.dumps(r.json()))


if __name__ == "__main__":
    cli()
