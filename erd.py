import re
import json
from types import FunctionType
from dataclasses import dataclass
from typing import Optional


class Parser:
    """
    A class to handle json files like manifest.json and catalog.json
    """
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
    """
    A class to handle the manifest.json file
    """
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
        nodes = {k: Node(k, self) for k, node in self["nodes"].items()
                 if node["resource_type"] == resource_type}
        if filter:
            nodes = {k: node for k, node in nodes.items() if filter(node)}

        return nodes
    

class Catalog(Parser):
    pass


@dataclass
class Node:
    """
    A class to represent a node (model, test, etc.) in the manifest. 
    """
    unique_id: str
    manifest: Manifest

    def __post_init__(self):
        if not self.validate():
            raise ValueError("Error validating this node")

    def __getitem__(self, key: str) -> any:
        return self.manifest["nodes"][self.unique_id].get(key)
    
    def validate(self):
        return True

    def is_unique_test(self):
        return self["resource_type"] == "test" and self["test_metadata"]["name"] == "unique"

    def is_not_null_test(self):
        return self["resource_type"] == "test" and self["test_metadata"]["name"] == "not_null"
    

class Test(Node):
    def validate(self):
        return self["resource_type"] == "test"

    def is_unique_test(self):
        return self["resource_type"] == "test" and self["test_metadata"]["name"] == "unique"

    def is_not_null_test(self):
        return self["resource_type"] == "test" and self["test_metadata"]["name"] == "not_null"


@dataclass
class Model(Node):
    """A class to represent a model node in the dbt manifest"""
    unique_id: str
    manifest: Manifest
    catalog: Catalog

    @property
    def columns(self) -> dict:
        return {name: Column(name, self) for name in self.catalog["nodes"][self.unique_id]["columns"]}
    
    @property
    def unique_columns(self):
        return {test["test_metadata"]["kwargs"]["column_name"] for test in self.unique_tests().values()}

    @property
    def not_null_columns(self):
        return {test["test_metadata"]["kwargs"]["column_name"] for test in self.not_null_tests().values()}

    def get_mermaid(self, indent=4):
        tab = " " * indent
        mermaid_elements = [f"{tab}{self['name']} {{"]
        mermaid_elements += [column.get_mermaid() for column in self.columns.values()]
        mermaid_elements.append(f"{tab}}}")
        mermaid = "\n".join(mermaid_elements)
        return mermaid
    
    def unique_tests(self):
        return self.manifest.get_nodes_by_type("test", lambda node: node.is_unique_test())

    def not_null_tests(self):
        return self.manifest.get_nodes_by_type("test", lambda node: node.is_not_null_test())

    def __repr__(self):
        return self["name"]


@dataclass
class Column:
    """A class to represent a column in a dbt model"""
    name: str
    model: Model

    def __getitem__(self, key):
        return self.model.catalog["nodes"][self.model.unique_id]["columns"][self.name][key]

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
        return f'{tab}{column_type} {column_name}{" PK" if self.is_primary_key else ""}'

    @property    
    def is_unique(self):
        return self.name in self.model.unique_columns

    @property
    def is_not_null(self):
        return self.name in self.model.not_null_columns

    @property
    def is_primary_key(self):
        return self.is_unique and self.is_not_null


@dataclass
class Dbt:
    manifest: Manifest
    catalog: Catalog

    def relationships(self) -> dict:
        relationship_tests = self.manifest.get_nodes_by_type(
            "test",
            lambda node: node["test_metadata"]["name"] == "relationships"
        )
        relationship_model_pairs = [test["depends_on"]["nodes"] for test in relationship_tests.values()]
        relationship_models = []

        for relationship_nodes in relationship_model_pairs:
            model_a_id, model_b_id = relationship_nodes
            model_a = Model(model_a_id, self.manifest, self.catalog)
            model_b = Model(model_b_id, self.manifest, self.catalog)
            relationship = Relationship(model_a, model_b)
            relationship_models.append(relationship)

        return relationship_models

    def models(self, select=""):
        # TODO: Figure out how we can delegate the select functionality to dbt
        match_fqn = select.split(".")
        selected_models = dict()
        for key, model in self.manifest.get_nodes_by_type("model").items():
            model = Model(key, self.manifest, self.catalog)
            if select == "" or model["fqn"][1:(len(match_fqn) + 1)] == match_fqn:
                selected_models[key] = model

        return selected_models

    def get_mermaid(self, show_fields=False, select=""):
        """Get the mermaid code for the ERD"""
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


@dataclass
class Relationship:
    model_a: Model
    model_b: Model
    relationship_type: Optional[str] = '||--o{'

    def get_mermaid(self, indent=4):
        tab = " " * indent
        mermaid = f'{tab}{self.model_a} {self.relationship_type} {self.model_b}: ""'
        return mermaid
