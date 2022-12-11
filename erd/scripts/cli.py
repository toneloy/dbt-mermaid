import click
from erd.erd import Dbt

@click.group()
def cli():
    pass


@cli.command
@click.argument("nodes", nargs=-1)
@click.option(
    "--manifest",
    "-m",
    "manifest_path",
    envvar="DBT_MANIFEST_PATH",
    default="target/manifest.json",
    help="Path to the dbt manifest.json")
@click.option(
    "--catalog",
    "-c",
    "catalog_path",
    envvar="DBT_CATALOG_PATH",
    default="target/catalog.json",
    help="Path to the dbt catalog.json")
@click.option(
    "--show-fields/--hide-fields",
    "-s/-h",
    is_flag=True,
    default=False,
    help="Show the table fields in the diagram?")
def erd(manifest_path, catalog_path, show_fields, nodes):
    dbt = Dbt(manifest_path, catalog_path)
    mermaid = dbt.get_mermaid(show_fields=show_fields, nodes=nodes)
    click.echo(mermaid)
