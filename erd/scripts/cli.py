import click
from erd.erd import Dbt

@click.group()
def cli():
    pass


@cli.command
@click.option(
    "--manifest-path",
    "-m",
    envvar="DBT_MANIFEST_PATH",
    help="Path to the dbt manifest.json")
@click.option(
    "--catalog-path",
    "-c",
    envvar="DBT_CATALOG_PATH",
    help="Path to the dbt catalog.json")
@click.option(
    "--show-fields/--hide-fields",
    is_flag=True,
    default=False,
    help="Show the table fields in the diagram?")
@click.option(
    "--select",
    default="",
    help="A selection of models to include in the diagram")
def erd(manifest_path, catalog_path, show_fields, select):

    dbt = Dbt(manifest_path, catalog_path)
    mermaid = dbt.get_mermaid(show_fields=show_fields, select=select)
    click.echo(mermaid)


if __name__ == "__main__":
    cli()
