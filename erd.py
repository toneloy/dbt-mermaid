import re
import json
from types import FunctionType


class Parser:
    data = None

    def __init__(self, path: str) -> None:
        self.path = path
        self.load()

    def load(self) -> None:
        with open(self.path, 'r') as f:
            self.data = json.load(f)

    def __getitem__(self, key: str) -> any:
        return self.data[key]


class Manifest(Parser):
    def get_nodes_by_type(
        self,
        resource_type: str,
        filter: FunctionType = None
    ) -> dict:
        """
        Get nodes of a certain type (model, test, etc.) from the manifest

        Args:
            resource_type: The type of resource to get
            filter: A function that takes the properties of a node and
                    returns True for selected models
        """
        nodes = {k: node for k, node in self["nodes"].items()
                 if node["resource_type"] == resource_type}
        if filter:
            nodes = {k: node for k, node in nodes.items() if filter(node)}

        return nodes
    

class Catalog(Parser):
    pass


class Node:
    properties = dict()

    def __init__(self, unique_id: str) -> None:
        self.unique_id = unique_id

    def load(self, manifest: Manifest) -> None:
        self.properties = manifest["nodes"][self.unique_id]
    
    def __getitem__(self, key: str) -> any:
        return self.properties.get(key)


class Column:
    def __init__(self, properties):
        self.properties = properties

    def __getitem__(self, key):
        return self.properties[key]

    def clean_property(self, property):
        """Clean a property according to mermaid specifications"""
        cleaned_name = re.sub("^([^a-zA-Z])+", "", self[property])
        cleaned_name = re.sub("([^a-zA-Z0-9_])+", "_", cleaned_name)
        return cleaned_name
    
    def get_mermaid(self, indent=4):
        """Get the mermaid representation"""
        tab = " " * indent * 2
        column_type = self.clean_property("type")
        column_name = self.clean_property("name")
        return f'{tab}{column_type} {column_name}'


class Model(Node):
    def load(self, manifest, catalog):
        super().load(manifest)
        self.properties["catalog"] = catalog["nodes"][self.unique_id]

    @property
    def columns(self) -> dict:
        return {k: Column(v) for k, v in self["catalog"]["columns"].items()}
    
    def get_mermaid(self, indent=4):
        tab = " " * indent
        mermaid_elements = [f"{tab}{self['name']} {{"]
        mermaid_elements += [column.get_mermaid() for column in self.columns.values()]
        mermaid_elements.append(f"{tab}}}")
        mermaid = "\n".join(mermaid_elements)
        return mermaid
    
    def __repr__(self):
        return self["name"]


class Dbt:
    def __init__(self, manifest: Manifest, catalog: Catalog) -> None:
        self.manifest = manifest
        self.catalog = catalog

    def relationships(self) -> dict:
        relationship_tests = self.manifest.get_nodes_by_type(
            "test",
            lambda node: node["test_metadata"]["name"] == "relationships"
        )
        relationship_model_pairs = [test["depends_on"]["nodes"] for test in relationship_tests.values()]
        relationship_models = []

        for relationship_nodes in relationship_model_pairs:
            model_a_id, model_b_id = relationship_nodes
            model_a, model_b = Model(model_a_id), Model(model_b_id)
            model_a.load(self.manifest, self.catalog)
            model_b.load(self.manifest, self.catalog)
            relationship_models.append(Relationship(model_a, model_b))

        return relationship_models

    def models(self, select=""):
        match_fqn = select.split(".")
        selected_models = dict()
        for key, model in self.manifest.get_nodes_by_type("model").items():
            model = Model(key)
            model.load(self.manifest, self.catalog)
            if select == "" or model["fqn"][1:(len(match_fqn) + 1)] == match_fqn:
                selected_models[key] = model

        return selected_models

    def get_mermaid(self, show_fields=False, select=""):
        """
        Get the mermaid code for the ERD
        """
        mermaid_lines = ["```mermaid", "erDiagram"]
        mermaid_relationships_list = [relationship.get_mermaid() for relationship in self.relationships()]
        mermaid_lines += mermaid_relationships_list

        if show_fields:
            catalog_mermaid_list = [model.get_mermaid() for model in self.models(select).values()]
            mermaid_catalog = "\n".join(catalog_mermaid_list)
            mermaid_lines.append(mermaid_catalog)
        mermaid_lines.append("```")
        mermaid = "\n".join(mermaid_lines)
        return mermaid


class Relationship:
    def __init__(
        self,
        model_a: Model,
        model_b: Model,
        relationship_type: str = "||--o{"
    ) -> None:
        self.model_a = model_a
        self.model_b = model_b
        self.relationship_type = relationship_type

    def get_mermaid(self, indent=4):
        tab = " " * indent
        mermaid = f'{tab}{self.model_a} {self.relationship_type} {self.model_b}: ""'
        return mermaid
