import re
import json
from types import FunctionType
from dataclasses import dataclass
from typing import Optional


@dataclass
class Dbt:
    """
    This class represents a dbt project from its manifest and catalog
    """
    manifest_path: str
    catalog_path: Optional[str] = ""

    def __post_init__(self):
        self.load_manifest()

        if self.catalog_path:
            self.load_catalog()

    def load_manifest(self):
        with open(self.manifest_path, 'r') as f:
            self.manifest = json.load(f)

    def load_catalog(self):
        with open(self.catalog_path, 'r') as f:
            self.catalog = json.load(f)

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
        node_classes = {
            "model": Model,
            "test": Test
        }
        node_class = node_classes.get(resource_type, Node)
        nodes = {k: node_class(k, self) for k, node in self.manifest["nodes"].items()
                 if node["resource_type"] == resource_type}
        if filter:
            nodes = {k: node for k, node in nodes.items() if filter(node)}

        return nodes

    def get_tests_by_type(self, test_type):
        """
        Get tests of a certain type (relationships, unique, not_null, etc.)
        """
        return {k: test for k, test in self.tests().items()
                if test.get("test_metadata", {}).get("name") == test_type}

    @property
    def tests(self):
        tests = self.get_nodes_by_type("test")
        return {k: Test(k, self) for k in tests}

    @staticmethod
    def get_name_from_path(nodes):
        return [node.split(".")[-1] for node in nodes]

    def relationships(self, nodes=None):
        if nodes:
            return {k: RelationshipTest(k, self) for k, node in self.tests.items()
                    if node.is_relationship and node["name"] in nodes}
        return {k: RelationshipTest(k, self) for k, node in self.tests.items()
            if node.is_relationship}

    def models(self, nodes=None):
        return self.get_nodes_by_type("model", lambda model: not nodes or model["name"] in nodes)

    def get_mermaid(self, nodes=None, show_fields=False):
        """Get the mermaid code for the ERD"""
        unique_ids = self.get_name_from_path(nodes)
        mermaid_lines = ["erDiagram"]
        mermaid_relationships_list = [relationship.get_mermaid()
                                      for relationship
                                      in self.relationships(unique_ids).values()]
        mermaid_lines += mermaid_relationships_list

        if show_fields:
            catalog_mermaid_list = [model.get_mermaid() for model in self.models(unique_ids).values()]
            mermaid_catalog = "\n".join(catalog_mermaid_list)
            mermaid_lines.append(mermaid_catalog)
        mermaid = "\n".join(mermaid_lines)
        return mermaid


@dataclass
class Node:
    """
    A class to represent a node (model, test, etc.) in the manifest.
    """
    unique_id: str
    project: Dbt

    def __post_init__(self):
        if not self.validate():
            raise ValueError("Error validating this node")

    def __getitem__(self, key: str) -> any:
        return self.project.manifest["nodes"][self.unique_id].get(key)

    def get(self, key, default=None):
        return self.project.manifest["nodes"][self.unique_id].get(key, default)

    def validate(self):
        return True


class Test(Node):
    def validate(self):
        return self["resource_type"] == "test"

    @property
    def is_unique_test(self):
        return self.get("test_metadata", {}).get("name") == "unique"

    @property
    def is_not_null_test(self):
        return self.get("test_metadata", {}).get("name") == "not_null"

    @property
    def is_relationship(self):
        return self.get("test_metadata", {}).get("name") == "relationships"


class RelationshipTest(Test):
    @property
    def models(self):
        return [Model(unique_id, self.project)
                for unique_id in self["depends_on"]["nodes"]]

    @property
    def model_a(self):
        return self.models[0]

    @property
    def model_b(self):
        return self.models[1]

    @property
    def foreign_key(self):
        return Column(
            self["test_metadata"]["kwargs"]["column_name"],
            self.model_b
        )

    @property
    def to(self):
        return Column(
            self["test_metadata"]["kwargs"]["field"],
            self.model_a
        )

    @property
    def cardinality_left(self):
        return "||" if self.foreign_key.is_not_null else '|o'

    @property
    def cardinality_right(self):
        return 'o|' if self.foreign_key.is_unique else 'o{'

    @property
    def relationship_type(self):
        return f'{self.cardinality_left}--{self.cardinality_right}'

    def get_mermaid(self):
        return f'{self.model_a} {self.relationship_type} {self.model_b}: ""'


@dataclass
class Model(Node):
    """A class to represent a model node in the dbt manifest"""
    unique_id: str
    project: Dbt

    @property
    def catalog(self):
        return self.project.catalog["nodes"][self.unique_id]

    @property
    def columns(self) -> dict:
        return {name: Column(name, self) for name in self.catalog["columns"]}

    @property
    def unique_columns(self):
        return {test["test_metadata"]["kwargs"]["column_name"]
                for test in self.unique_tests.values()}

    @property
    def not_null_columns(self):
        return {test["test_metadata"]["kwargs"]["column_name"]
                for test in self.not_null_tests.values()}

    def get_mermaid(self):
        mermaid_elements = [f"{self['name']} {{"]
        mermaid_elements += [column.get_mermaid()
                             for column in self.columns.values()]
        mermaid_elements.append("}")
        mermaid = "\n".join(mermaid_elements)
        return mermaid

    @property
    def tests(self):
        return {k: test for k, test in self.project.tests.items()
                if self.unique_id in test["depends_on"]["nodes"]}

    def is_related_test(self, node):
        return node.unique_id in self.tests

    @property
    def unique_tests(self):
        return {k: node for k, node in self.tests.items()
                if node.is_unique_test}

    @property
    def not_null_tests(self):
        return {k: node for k, node in self.tests.items()
                if node.is_not_null_test}

    def __repr__(self):
        return self["name"]


@dataclass
class Column:
    """A class to represent a column in a dbt model"""
    name: str
    model: Model

    def __getitem__(self, key):
        return self.model.catalog["columns"][self.name][key]

    def clean_property(self, property):
        """Clean a property according to mermaid specifications"""
        cleaned_name = re.sub("^([^a-zA-Z])+", "", self[property])
        cleaned_name = re.sub("([^a-zA-Z0-9_])+", "_", cleaned_name)
        return cleaned_name

    def get_mermaid(self, indent=4):
        """Get the mermaid representation"""
        tab = " " * indent
        column_type = self.clean_property("type")
        column_name = self.clean_property("name")
        pk_marker = " PK" if self.is_primary_key else ""
        return f'{tab}{column_type} {column_name}{pk_marker}'

    @property
    def is_unique(self):
        return self.name in self.model.unique_columns

    @property
    def is_not_null(self):
        return self.name in self.model.not_null_columns

    @property
    def is_primary_key(self):
        return self.is_unique and self.is_not_null
