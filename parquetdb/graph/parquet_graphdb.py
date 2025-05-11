import json
import logging
import os
import shutil
from pathlib import Path
from typing import Callable, Dict, List, Type, Union

import pyarrow as pa
import pyarrow.compute as pc

from parquetdb import ParquetDB
from parquetdb.graph.edges import EdgeStore
from parquetdb.graph.generator_store import GeneratorStore
from parquetdb.graph.nodes import NodeStore
from parquetdb.utils.log_utils import set_verbose_level

logger = logging.getLogger(__name__)


class ParquetGraphDB:
    """
    A manager for a graph storing multiple node types and edge types.
    Each node type and edge type is backed by a separate ParquetDB instance
    (wrapped by NodeStore or EdgeStore).
    """

    def __init__(
        self,
        storage_path: Union[str, Path],
        node_store_types: Dict[str, Type[NodeStore]] = None,
        edge_store_types: Dict[str, Type[EdgeStore]] = None,
        verbose: int = 1,
    ):
        """
        Parameters
        ----------
        storage_path : str
            The root path for this graph, e.g. '/path/to/my_graph'.
            Subdirectories 'nodes/' and 'edges/' will be used.
        """
        self.verbose = verbose
        set_verbose_level(verbose)

        logger.info(f"Initializing GraphDB at root path: {storage_path}")
        self.storage_path = (
            Path(storage_path) if isinstance(storage_path, str) else storage_path
        )

        self.nodes_path = self.storage_path / "nodes"
        self.edges_path = self.storage_path / "edges"
        self.edge_generators_path = self.storage_path / "edge_generators"
        self.node_generators_path = self.storage_path / "node_generators"
        self.graph_path = self.storage_path / "graph"
        self.generator_dependency_json = self.storage_path / "generator_dependency.json"

        self.graph_name = self.storage_path.name

        # Create directories if they don't exist
        self.nodes_path.mkdir(parents=True, exist_ok=True)
        self.edges_path.mkdir(parents=True, exist_ok=True)
        self.edge_generators_path.mkdir(parents=True, exist_ok=True)
        self.graph_path.mkdir(parents=True, exist_ok=True)

        logger.debug(f"Node directory: {self.nodes_path}")
        logger.debug(f"Edge directory: {self.edges_path}")
        logger.debug(f"Graph directory: {self.graph_path}")

        #  Initialize empty dictionaries for stores, load existing stores
        self.node_stores = {}
        self.edge_stores = {}

        node_store_types = {} if node_store_types is None else node_store_types
        edge_store_types = {} if edge_store_types is None else edge_store_types

        for store_type in list(self.nodes_path.iterdir()):
            if store_type.is_dir():
                if store_type in node_store_types:
                    self.node_stores[store_type.name] = node_store_types[
                        store_type.name
                    ](store_type)
                else:
                    self.node_stores[store_type.name] = NodeStore(store_type)

        for store_type in list(self.edges_path.iterdir()):
            if store_type.is_dir():
                if store_type in edge_store_types:
                    self.edge_stores[store_type.name] = edge_store_types[
                        store_type.name
                    ](store_type)
                else:
                    self.edge_stores[store_type.name] = EdgeStore(store_type)

        self.edge_generator_store = GeneratorStore(
            storage_path=self.edge_generators_path, verbose=self.verbose
        )
        self.node_generator_store = GeneratorStore(
            storage_path=self.node_generators_path, verbose=self.verbose
        )

        # This is here to make sure the node and edges paths
        # listed in the generator stores align where the GraphDB is,
        # this allows user to easily move the directory and the generators will still work
        self._load_generator_dependency_graph()
        self.generator_consistency_check()

    def __repr__(self):
        return self.summary(show_column_names=False)

    # def __getitem__(self, *args: QueryType) -> Any:
    #     # `data[*]` => Link to either `_global_store`, _node_store_dict` or
    #     # `_edge_store_dict`.
    #     # If neither is present, we create a new `Storage` object for the given
    #     # node/edge-type.
    #     key = self._to_canonical(*args)

    #     out = self._global_store.get(key, None)
    #     if out is not None:
    #         return out

    #     if isinstance(key, tuple):
    #         return self.get_edge_store(*key)
    #     else:
    #         return self.get_node_store(key)

    # def __setitem__(self, key: str, value: Any):
    #     if key in self.node_types:
    #         raise AttributeError(f"'{key}' is already present as a node type")
    #     elif key in self.edge_types:
    #         raise AttributeError(f"'{key}' is already present as an edge type")
    #     self._global_store[key] = value

    # def __delitem__(self, *args: QueryType):
    #     # `del data[*]` => Link to `_node_store_dict` or `_edge_store_dict`.
    #     key = self._to_canonical(*args)
    #     if key in self.edge_types:
    #         del self._edge_store_dict[key]
    #     elif key in self.node_types:
    #         del self._node_store_dict[key]
    #     else:
    #         del self._global_store[key]

    @property
    def n_node_types(self):
        return len(self.node_stores)

    @property
    def n_edge_types(self):
        return len(self.edge_stores)

    @property
    def n_nodes_per_type(self):
        return {
            node_type: node_store.n_nodes
            for node_type, node_store in self.node_stores.items()
        }

    @property
    def n_edges_per_type(self):
        return {
            edge_type: edge_store.n_edges
            for edge_type, edge_store in self.edge_stores.items()
        }

    def generator_consistency_check(self):
        logger.info("Checking directory consistency")
        self._generator_check(self.node_generator_store)
        self._generator_check(self.edge_generator_store)

    def _generator_check(self, generator_store):
        df = generator_store.read().to_pandas()
        for i, row in df.iterrows():
            generator_name = row["generator_name"]
            for col_name in df.columns:
                if col_name.startswith("generator_kwargs.") or col_name.startswith(
                    "generator_args."
                ):
                    col_value = row[col_name]
                    if isinstance(col_value, (EdgeStore, NodeStore)):
                        store = col_value

                        if hasattr(store, "node_type"):
                            current_path = self.get_node_store(
                                store.node_type
                            ).storage_path
                            generator_store_path = store.storage_path
                            if current_path != generator_store_path:
                                df.at[i, col_name] = current_path

                        elif hasattr(store, "edge_type"):
                            current_path = self.get_edge_store(
                                store.edge_type
                            ).storage_path
                            generator_store_path = store.storage_path
                            if current_path != generator_store_path:
                                df.at[i, col_name] = current_path

        generator_store.update(data=df)

    def _load_generator_dependency_graph(self):
        if os.path.exists(self.generator_dependency_json):
            with open(self.generator_dependency_json, "r") as f:
                self.generator_dependency_graph = json.load(f)
                for key, value in self.generator_dependency_graph["nodes"].items():
                    self.generator_dependency_graph["nodes"][key] = set(value)
                for key, value in self.generator_dependency_graph["edges"].items():
                    self.generator_dependency_graph["edges"][key] = set(value)
        else:
            self.generator_dependency_graph = {"nodes": {}, "edges": {}}

    def summary(self, show_column_names: bool = False):
        # Header section
        tmp_str = f"{'=' * 60}\n"
        tmp_str += f"GRAPH DATABASE SUMMARY\n"
        tmp_str += f"{'=' * 60}\n"
        tmp_str += f"Name: {self.graph_name}\n"
        tmp_str += f"Storage path: {os.path.abspath(self.storage_path)}\n"
        tmp_str += "└── Repository structure:\n"
        tmp_str += (
            f"    ├── nodes/                 ({os.path.abspath(self.nodes_path)})\n"
        )
        tmp_str += (
            f"    ├── edges/                 ({os.path.abspath(self.edges_path)})\n"
        )
        tmp_str += f"    ├── edge_generators/       ({os.path.abspath(self.edge_generators_path)})\n"
        tmp_str += f"    ├── node_generators/       ({os.path.abspath(self.node_generators_path)})\n"
        tmp_str += (
            f"    └── graph/                 ({os.path.abspath(self.graph_path)})\n\n"
        )

        # Node section header
        tmp_str += f"{'#' * 60}\n"
        tmp_str += f"NODE DETAILS\n"
        tmp_str += f"{'#' * 60}\n"
        tmp_str += f"Total node types: {len(self.node_stores)}\n"
        tmp_str += f"{'-' * 60}\n"

        # Node details
        for node_type, node_store in self.node_stores.items():
            tmp_str += f"• Node type: {node_type}\n"
            tmp_str += f"  - Number of nodes: {node_store.n_nodes}\n"
            tmp_str += f"  - Number of features: {node_store.n_features}\n"
            if show_column_names:
                tmp_str += f"  - Columns:\n"
                for col in node_store.columns:
                    tmp_str += f"       - {col}\n"
            tmp_str += f"  - db_path: {os.path.abspath(node_store.storage_path)}\n"
            tmp_str += f"{'-' * 60}\n"

        # Edge section header
        tmp_str += f"\n{'#' * 60}\n"
        tmp_str += f"EDGE DETAILS\n"
        tmp_str += f"{'#' * 60}\n"
        tmp_str += f"Total edge types: {len(self.edge_stores)}\n"
        tmp_str += f"{'-' * 60}\n"

        # Edge details
        for edge_type, edge_store in self.edge_stores.items():
            tmp_str += f"• Edge type: {edge_type}\n"
            tmp_str += f"  - Number of edges: {edge_store.n_edges}\n"
            tmp_str += f"  - Number of features: {edge_store.n_features}\n"
            if show_column_names:
                tmp_str += f"  - Columns:\n"
                for col in edge_store.columns:
                    tmp_str += f"       - {col}\n"
            tmp_str += f"  - db_path: {os.path.abspath(edge_store.storage_path)}\n"
            tmp_str += f"{'-' * 60}\n"

        # Node generator header
        tmp_str += f"\n{'#' * 60}\n"
        tmp_str += f"NODE GENERATOR DETAILS\n"
        tmp_str += f"{'#' * 60}\n"
        tmp_str += f"Total node generators: {self.node_generator_store.n_generators}\n"
        tmp_str += f"{'-' * 60}\n"

        # Node generator details
        for generator_name in self.node_generator_store.generator_names:
            df = self.node_generator_store.load_generator_data(generator_name)
            tmp_str += f"• Generator: {generator_name}\n"
            tmp_str += f"Generator Args:\n"
            for col in df.columns:
                col_name = col.replace("generator_args.", "")
                if isinstance(df[col].iloc[0], (NodeStore, EdgeStore)):
                    tmp_str += f"  - {col_name}: {os.path.abspath(df[col].iloc[0].storage_path)}\n"
                else:
                    tmp_str += f"  - {col_name}: {df[col].tolist()}\n"
            tmp_str += f"Generator Kwargs:\n"
            for col in df.columns:
                col_name = col.replace("generator_kwargs.", "")
                if col.startswith("generator_kwargs."):
                    tmp_str += f"  - {col_name}: {df[col].tolist()}\n"
            tmp_str += f"{'-' * 60}\n"

        # Edge generator header
        tmp_str += f"\n{'#' * 60}\n"
        tmp_str += f"EDGE GENERATOR DETAILS\n"
        tmp_str += f"{'#' * 60}\n"
        tmp_str += f"Total edge generators: {self.edge_generator_store.n_generators}\n"
        tmp_str += f"{'-' * 60}\n"

        # Edge generator details
        for generator_name in self.edge_generator_store.generator_names:
            df = self.edge_generator_store.load_generator_data(generator_name)
            tmp_str += f"• Generator: {generator_name}\n"
            tmp_str += f"Generator Args:\n"
            for col in df.columns:
                col_name = col.replace("generator_args.", "")
                if col.startswith("generator_args."):
                    if isinstance(df[col].iloc[0], (NodeStore, EdgeStore)):
                        tmp_str += f"  - {col_name}: {os.path.abspath(df[col].iloc[0].storage_path)}\n"
                    else:
                        tmp_str += f"  - {col_name}: {df[col].tolist()}\n"
            tmp_str += f"Generator Kwargs:\n"
            for col in df.columns:
                col_name = col.replace("generator_kwargs.", "")
                if col.startswith("generator_kwargs."):
                    tmp_str += f"  - {col_name}: {df[col].tolist()}\n"
            tmp_str += f"{'-' * 60}\n"

        return tmp_str

    # ------------------
    # Node-level methods
    # ------------------
    def add_nodes(self, node_type: str, data, **kwargs):
        logger.info(f"Creating nodes of type '{node_type}'")
        store = self.add_node_type(node_type)
        store.create_nodes(data, **kwargs)

        self._run_dependent_generators(node_type)
        logger.debug(f"Successfully created nodes of type '{node_type}'")

    def add_node_type(self, node_type: str) -> NodeStore:
        """
        Create (or load) a NodeStore for the specified node_type.
        """
        if node_type in self.node_stores:
            logger.debug(f"Returning existing NodeStore for type: {node_type}")
            return self.node_stores[node_type]

        logger.info(f"Creating new NodeStore for type: {node_type}")
        storage_path = self.nodes_path / node_type
        self.node_stores[node_type] = NodeStore(
            storage_path=storage_path, verbose=self.verbose
        )
        return self.node_stores[node_type]

    def add_node_store(
        self,
        node_store: NodeStore,
        overwrite: bool = False,
        remove_original: bool = False,
    ):
        logger.info(f"Adding node store of type {node_store.node_type}")

        # Check if node store already exists
        if node_store.node_type in self.node_stores:
            if overwrite:
                logger.warning(
                    f"Node store of type {node_store.node_type} already exists, overwriting"
                )
                self.remove_node_store(node_store.node_type)
            else:
                raise ValueError(
                    f"Node store of type {node_store.node_type} already exists, and overwrite is False"
                )

        # Move node store to the nodes directory
        new_path = self.nodes_path / node_store.node_type
        if node_store.storage_path != new_path:
            logger.debug(
                f"Moving node store from {node_store.storage_path} to {new_path}"
            )
            shutil.copytree(node_store.storage_path, new_path)

            if remove_original:
                shutil.rmtree(node_store.storage_path)
            node_store.storage_path = new_path
        self.node_stores[node_store.node_type] = node_store

        self._run_dependent_generators(node_store.node_type)

    def get_nodes(
        self, node_type: str, ids: List[int] = None, columns: List[str] = None, **kwargs
    ):
        logger.info(f"Reading nodes of type '{node_type}'")
        if ids:
            logger.debug(f"Filtering by {len(ids)} node IDs")
        if columns:
            logger.debug(f"Selecting columns: {columns}")
        store = self.get_node_store(node_type)
        return store.read_nodes(ids=ids, columns=columns, **kwargs)

    def read_nodes(
        self, node_type: str, ids: List[int] = None, columns: List[str] = None, **kwargs
    ):
        store = self.get_node_store(node_type)
        return store.read_nodes(ids=ids, columns=columns, **kwargs)

    def get_node_store(self, node_type: str):
        # if node_type not in self.node_stores:
        node_store = self.node_stores.get(node_type, None)
        if node_store is None:
            raise ValueError(f"Node store of type {node_type} does not exist")
        return node_store

    def update_nodes(self, node_type: str, data, **kwargs):
        store = self.get_node_store(node_type)
        store.update_nodes(data, **kwargs)

        self._run_dependent_generators(node_type)

    def delete_nodes(
        self, node_type: str, ids: List[int] = None, columns: List[str] = None
    ):
        store = self.get_node_store(node_type)
        store.delete_nodes(ids=ids, columns=columns)

        self._run_dependent_generators(node_type)

    def remove_node_store(self, node_type: str):
        logger.info(f"Removing node store of type {node_type}")
        store = self.get_node_store(node_type)
        shutil.rmtree(store.storage_path)
        self.node_stores.pop(node_type)

        self._run_dependent_generators(node_type)

    def remove_node_type(self, node_type: str):
        self.remove_node_store(node_type)

    def normalize_nodes(self, node_type: str, normalize_kwargs: Dict = None):
        store = self.add_node_type(node_type)
        store.normalize_nodes(**normalize_kwargs)

    def normalize_all_nodes(self, normalize_kwargs):
        for node_type in self.node_stores:
            self.normalize_nodes(node_type, normalize_kwargs)

    def list_node_types(self):
        return list(self.node_stores.keys())

    def node_exists(self, node_type: str):
        logger.debug(f"Node type: {node_type}")

        return node_type in self.node_stores

    def node_is_empty(self, node_type: str):
        store = self.get_node_store(node_type)
        return store.is_empty()

    def add_node_generator(
        self,
        generator_func: Callable,
        generator_args: Dict = None,
        generator_kwargs: Dict = None,
        create_kwargs: Dict = None,
        run_immediately: bool = True,
        run_generator_kwargs: Dict = None,
        depends_on: List[str] = None,
        add_dependency: bool = True,
    ) -> None:
        generator_name = generator_func.__name__
        self.node_generator_store.store_generator(
            generator_func=generator_func,
            generator_name=generator_name,
            generator_args=generator_args,
            generator_kwargs=generator_kwargs,
            create_kwargs=create_kwargs,
        )
        self.generator_consistency_check()

        if run_immediately:
            if run_generator_kwargs is None:
                run_generator_kwargs = dict(generator_name=generator_name)
            else:
                run_generator_kwargs["generator_name"] = generator_name

            self.run_node_generator(**run_generator_kwargs)

        if add_dependency and depends_on:
            self.add_generator_dependency(generator_name, depends_on, node_type="nodes")
        elif add_dependency and not depends_on:
            self.add_generator_dependency(generator_name)

    def run_node_generator(
        self,
        generator_name: str,
        generator_args: Dict = None,
        generator_kwargs: Dict = None,
        create_kwargs: Dict = None,
    ) -> pa.Table:
        """
        Execute a previously registered custom node-generation function by name.
        Parameters
        ----------
        generator_name : str
            The unique name used when registering the function.
        generator_args : Dict
            Additional arguments passed to the generator function.
        generator_kwargs : Dict
            Additional keyword arguments passed to the generator function.

        Raises
        ------
        ValueError
            If there is no generator function with the given name.
        """
        if create_kwargs is None:
            create_kwargs = {}

        if generator_args is None:
            generator_args = {}

        if generator_kwargs is None:
            generator_kwargs = {}

        table = self.node_generator_store.run_generator(
            generator_name,
            generator_args=generator_args,
            generator_kwargs=generator_kwargs,
        )

        storage_path = os.path.join(self.nodes_path, generator_name)
        if os.path.exists(storage_path):
            logger.info(f"Removing existing node store: {generator_name}")
            self.remove_node_store(generator_name)

        self.add_nodes(node_type=generator_name, data=table, **create_kwargs)
        return table

    # ------------------
    # Edge-level methods
    # ------------------
    def add_edge_type(self, edge_type: str) -> EdgeStore:
        """
        Create (or load) an EdgeStore for the specified edge_type.
        """
        if edge_type in self.edge_stores:
            logger.debug(f"Returning existing EdgeStore for type: {edge_type}")
            return self.edge_stores[edge_type]

        logger.info(f"Creating new EdgeStore for type: {edge_type}")
        storage_path = self.edges_path / edge_type
        self.edge_stores[edge_type] = EdgeStore(
            storage_path=storage_path, verbose=self.verbose
        )
        return self.edge_stores[edge_type]

    def add_edges(self, edge_type: str, data, **kwargs):
        logger.info(f"Creating edges of type '{edge_type}'")
        incoming_table = ParquetDB.construct_table(data)
        self._validate_edge_references(incoming_table)
        store = self.add_edge_type(edge_type)
        store.create_edges(incoming_table, **kwargs)
        self._run_dependent_generators(edge_type)
        logger.debug(f"Successfully created edges of type '{edge_type}'")

    def add_edge_store(self, edge_store: EdgeStore):
        logger.info(f"Adding edge store of type {edge_store.edge_type}")

        # Move edge store to the edges directory
        new_path = self.edges_path / edge_store.edge_type
        if edge_store.storage_path != new_path:
            logger.debug(
                f"Moving edge store from {edge_store.storage_path} to {new_path}"
            )

            new_path.mkdir(parents=True, exist_ok=True)
            for file in edge_store.storage_path.glob("*"):
                new_file = new_path / file.name
                shutil.move(file, new_file)
            edge_store.storage_path = new_path
        self.edge_stores[edge_store.edge_type] = edge_store

        self._run_dependent_generators(edge_store.edge_type)

    def read_edges(
        self, edge_type: str, ids: List[int] = None, columns: List[str] = None, **kwargs
    ):
        store = self.add_edge_type(edge_type)
        return store.read_edges(ids=ids, columns=columns, **kwargs)

    def update_edges(self, edge_type: str, data, **kwargs):
        store = self.add_edge_type(edge_type)
        store.update_edges(data, **kwargs)

        self._run_dependent_generators(edge_type)

    def delete_edges(
        self, edge_type: str, ids: List[int] = None, columns: List[str] = None
    ):
        store = self.add_edge_type(edge_type)
        store.delete_edges(ids=ids, columns=columns)

        self._run_dependent_generators(edge_type)

    def remove_edge_store(self, edge_type: str):
        logger.info(f"Removing edge store of type {edge_type}")
        store = self.get_edge_store(edge_type)
        shutil.rmtree(store.storage_path)
        self.edge_stores.pop(edge_type)

        self._run_dependent_generators(edge_type)

    def remove_edge_type(self, edge_type: str):
        self.remove_edge_store(edge_type)

    def normalize_edges(self, edge_type: str):
        store = self.add_edge_type(edge_type)
        store.normalize_edges()

    def normalize_all_edges(self, normalize_kwargs):
        for edge_type in self.edge_stores:
            self.normalize_edges(edge_type, normalize_kwargs)

    def get_edge_store(self, edge_type: str):
        edge_store = self.edge_stores.get(edge_type, None)
        if edge_store is None:
            raise ValueError(f"Edge store of type {edge_type} does not exist")
        return edge_store

    def list_edge_types(self):
        return list(self.edge_stores.keys())

    def edge_exists(self, edge_type: str):
        return edge_type in self.edge_stores

    def edge_is_empty(self, edge_type: str):
        store = self.get_edge_store(edge_type)
        return store.is_empty()

    def _validate_edge_references(self, table: pa.Table) -> None:
        """
        Checks whether source_id and target_id in each edge record exist
        in the corresponding node stores.

        Parameters
        ----------
        table : pa.Table
            A table containing 'source_id' and 'target_id' columns.
        source_node_type : str
            The node type for the source nodes (e.g., 'user').
        target_node_type : str
            The node type for the target nodes (e.g., 'item').

        Raises
        ------
        ValueError
            If any source_id/target_id is not found in the corresponding node store.
        """
        # logger.debug(f"Validating edge references: {source_node_type} -> {target_node_type}")
        edge_table = table
        # 1. Retrieve the NodeStores
        names = edge_table.column_names
        logger.debug(f"Column names: {names}")

        assert "source_type" in names, "source_type column not found in table"
        assert "target_type" in names, "target_type column not found in table"
        assert "source_id" in names, "source_id column not found in table"
        assert "target_id" in names, "target_id column not found in table"
        assert "edge_type" in names, "edge_type column not found in table"

        node_types = pc.unique(table["source_type"]).to_pylist()

        for node_type in node_types:
            store = self.node_stores.get(node_type, None)
            if store is None:
                logger.error(f"No node store found for node_type='{node_type}'")
                raise ValueError(f"No node store found for node_type='{node_type}'.")

            # Read all existing source IDs from store_1
            node_table = store.read_nodes(columns=["id"])

            # Filter all source_ids and target_ids that are of the same type as store_1
            source_id_array = edge_table.filter(
                pc.field("source_type") == store.node_type
            )["source_id"].combine_chunks()
            target_id_array = edge_table.filter(
                pc.field("target_type") == store.node_type
            )["target_id"].combine_chunks()

            all_source_type_ids = pa.concat_arrays([source_id_array, target_id_array])

            # Check if all source_ids and target_ids are in the node_store
            is_source_ids_in_source_store = pc.index_in(
                all_source_type_ids, node_table["id"]
            )
            invalid_source_ids = is_source_ids_in_source_store.filter(
                pc.is_null(is_source_ids_in_source_store)
            )

            if len(invalid_source_ids) > 0:
                raise ValueError(
                    f"Source IDs not found in source_store of type {store.node_type}: {invalid_source_ids}"
                )

        logger.debug("Edge reference validation completed successfully")

    def construct_table(self, data, schema=None, metadata=None, fields_metadata=None):
        logger.info("Validating data")
        return ParquetDB.construct_table(
            data, schema=schema, metadata=metadata, fields_metadata=fields_metadata
        )

    def add_edge_generator(
        self,
        generator_func: Callable,
        generator_args: Dict = None,
        generator_kwargs: Dict = None,
        create_kwargs: Dict = None,
        run_immediately: bool = True,
        run_generator_create_kwargs: Dict = None,
        depends_on: List[str] = None,
        add_dependency: bool = True,
    ) -> None:
        """
        Register a user-defined callable that can read from node stores,
        and then create or update edges as it sees fit.

        Parameters
        ----------
        name : str
            A unique identifier for this generator function.
        generator_func : Callable
        """
        generator_name = generator_func.__name__
        self.edge_generator_store.store_generator(
            generator_func=generator_func,
            generator_name=generator_name,
            generator_args=generator_args,
            generator_kwargs=generator_kwargs,
            create_kwargs=create_kwargs,
        )
        logger.info(f"Added new edge generator: {generator_name}")

        if run_immediately:
            if run_generator_create_kwargs is None:
                run_generator_create_kwargs = {}
            self.run_edge_generator(
                generator_name=generator_name, create_kwargs=run_generator_create_kwargs
            )

        if add_dependency and depends_on:
            self.add_generator_dependency(generator_name, depends_on, node_type="edges")
        elif add_dependency and not depends_on:
            self.add_generator_dependency(generator_name)

    def run_edge_generator(
        self,
        generator_name: str,
        generator_args: Dict = None,
        generator_kwargs: Dict = None,
        create_kwargs: Dict = None,
    ) -> None:
        if create_kwargs is None:
            create_kwargs = {}

        table = self.edge_generator_store.run_generator(
            generator_name,
            generator_args=generator_args,
            generator_kwargs=generator_kwargs,
        )

        storage_path = os.path.join(self.edges_path, generator_name)
        if os.path.exists(storage_path):
            logger.info(f"Removing existing edge store: {generator_name}")
            self.remove_edge_store(generator_name)

        self.add_edges(edge_type=generator_name, data=table, **create_kwargs)
        return table

    def get_generator_type(self, generator_name: str):
        if self.edge_generator_store.is_in(generator_name):
            return "edges"
        elif self.node_generator_store.is_in(generator_name):
            return "nodes"
        else:
            raise ValueError(f"Generator {generator_name} not in node or edge store")

    def get_generator_dependency_graph(self, generator_name: str):

        generator_type = self.get_generator_type(generator_name)
        generator_store = (
            self.edge_generator_store
            if generator_type == "edges"
            else self.node_generator_store
        )
        dependency_graph = {generator_type: {}}
        df = generator_store.load_generator_data(generator_name=generator_name)
        logger.debug(f"Generator data: {df.columns}")

        for i, row in df.iterrows():
            if generator_name not in dependency_graph[generator_type]:
                dependency_graph[generator_type][generator_name] = set()

            for col_name in df.columns:
                if col_name.startswith("generator_kwargs.") or col_name.startswith(
                    "generator_args."
                ):
                    col_value = row[col_name]
                    if isinstance(col_value, (EdgeStore, NodeStore)):
                        store = col_value

                        if hasattr(store, "node_type"):
                            current_path = self.get_node_store(
                                store.node_type
                            ).storage_path
                            dependency_graph[generator_type][generator_name].add(
                                store.node_type
                            )

                        elif hasattr(store, "edge_type"):
                            current_path = self.get_edge_store(
                                store.edge_type
                            ).storage_path

                            dependency_graph[generator_type][generator_name].add(
                                store.edge_type
                            )
        return dependency_graph

    def add_generator_dependency(
        self,
        generator_name: str,
        depends_on: List[str] = None,
        store_type: str = None,
    ):
        """
        Add dependencies for a generator. When any of the dependencies are updated,
        the generator will automatically run.

        Parameters
        ----------
        generator_name : str
            Name of the generator that has dependencies
        depends_on : List[str]
            List of store names that this generator depends on
        store_type : str
            Either 'nodes' or 'edges', indicating the type of store the generator creates
        """
        dependencies = None
        if depends_on:
            logger.info(f"Adding dependencies for {generator_name}: {depends_on}")
            if store_type not in ["nodes", "edges"]:
                raise ValueError("store_type must be either 'nodes' or 'edges'")
            if generator_name not in self.generator_dependency_graph[store_type]:
                self.generator_dependency_graph[store_type][generator_name] = set()
            dependencies = depends_on
            self.generator_dependency_graph[store_type][generator_name].update(
                dependencies
            )
        else:
            logger.info(f"Adding all dependencies for {generator_name}")
            dependencies = self.get_generator_dependency_graph(generator_name)
            self.generator_dependency_graph.update(dependencies)

        with open(self.generator_dependency_json, "w") as f:
            for key, value in self.generator_dependency_graph["nodes"].items():
                self.generator_dependency_graph["nodes"][key] = list(value)
            for key, value in self.generator_dependency_graph["edges"].items():
                self.generator_dependency_graph["edges"][key] = list(value)
            json.dump(self.generator_dependency_graph, f)

        if dependencies:
            logger.info(f"Added dependencies for {generator_name}: {dependencies}")

    def _run_dependent_generators(self, store_name: str):
        """
        Run all generators that depend on a specific store.

        Parameters
        ----------
        store_type : str
            Either 'nodes' or 'edges'
        store_name : str
            Name of the store that was updated
        """
        logger.info(f"Running dependent generators: {store_name}")

        # Find all generators that depend on this store
        dependent_generators = set()
        for generator_name, dependencies in self.generator_dependency_graph[
            "nodes"
        ].items():
            if store_name in dependencies:
                dependent_generators.add(("nodes", generator_name))

        for generator_name, dependencies in self.generator_dependency_graph[
            "edges"
        ].items():
            if store_name in dependencies:
                dependent_generators.add(("edges", generator_name))

        logger.debug(f"Dependent generators: {dependent_generators}")
        # Run each dependent generator
        for dep_store_type, generator_name in dependent_generators:
            logger.info(f"Running dependent generator: {generator_name}")
            try:
                if dep_store_type == "nodes":
                    self.run_node_generator(generator_name)
                    # Recursively run generators that depend on this result
                    self._run_dependent_generators(generator_name)
                else:
                    self.run_edge_generator(generator_name=generator_name)
                    # Recursively run generators that depend on this result
                    self._run_dependent_generators(generator_name)
            except Exception as e:
                logger.error(
                    f"Failed to run dependent generator {generator_name}: {str(e)}"
                )
