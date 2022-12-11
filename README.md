# dbt-mermaid

## Installation
For now, you'd have to install from GitHub by executing

```bash
pip install git+https://github.com/toneloy/dbt-mermaid.git
```

## Usage
Before running the `erd` command, make sure that your `manifest.json` and `catalog.json` files are up-to-date by running

```bash
dbt docs generate
```

This will usually create both files in the `target` folder of your dbt project.

### Generating an ERD of related models

In your dbt project folder, just run

```bash
erd
```

You can also specify the path to the `manifest.json` and `catalog.json` files:
1. With the options `--manifest` and `--catalog`. By default, the `erd` command will assume they're in the `target` folder.
1. Setting the `DBT_MANIFEST_PATH` and `DBT_CATALOG_PATH` environment variables

### Showing the models fields

If you also want to show the fields in each model, use the `-s` or `--show-fields` option.

**Note:** This will include all models, regardless of them having or not a relationship to another model. If you want to select only specific models, see the **Selecting models/relationships** section

```bash
erd -s
```

### Selecting models/relationships

You can specify which models/tests you want to include in the diagram by specifying them as arguments.

```bash
erd [model_1 model_2 ...] [test_1 test_2 ...]
```

As a shortcut, you can use the output of `dbt list --select <selection-criteria>`. This will help you if you want to show the diagram for models in a specific folder or models that have a specific tag.

```bash
erd $(dbt list --select <selection-criteria>)
```
