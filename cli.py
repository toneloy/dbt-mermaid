import json
import click
from erd import Dbt, Manifest, Catalog

@click.group()
def cli():
    pass


@cli.command
@click.option("--manifest-path", envvar="DBT_MANIFEST_PATH")
@click.option("--catalog-path", envvar="DBT_CATALOG_PATH")
@click.option("--show-fields/--hide-fields", is_flag=True, default=False)
@click.option("--select", default="")
def erd(manifest_path, catalog_path, show_fields, select):

    manifest = Manifest(manifest_path)
    catalog = Catalog(catalog_path)

    dbt = Dbt(
        manifest=manifest,
        catalog=catalog
    )
    mermaid = dbt.get_mermaid(show_fields=show_fields, select=select)
    click.echo(mermaid)


if __name__ == "__main__":
    cli()
