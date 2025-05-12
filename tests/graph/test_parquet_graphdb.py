import logging
import os
import shutil

import pandas as pd
import pyarrow as pa
import pytest
from utils import element, element_element_neighborsByGroupPeriod

from parquetdb.graph import EdgeStore, NodeStore, ParquetGraphDB
from parquetdb.utils.log_utils import set_verbose_level

logger = logging.getLogger(__name__)

VERBOSE = 1

pytestmark = pytest.mark.filterwarnings(
    "ignore:__array__ implementation doesn't accept a copy keyword.*:DeprecationWarning"
)


@pytest.fixture
def tmp_dir(tmp_path):
    """Fixture for temporary directory."""
    tmp_dir = str(tmp_path)
    yield tmp_dir
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)


@pytest.fixture
def graphdb(tmp_dir):
    """Fixture to create a GraphDB instance."""
    tmp_dir = os.path.join(tmp_dir, "GraphDB")
    return ParquetGraphDB(storage_path=tmp_dir, verbose=VERBOSE)


@pytest.fixture
def element_store(tmp_dir):
    """Fixture to create an ElementNodes instance."""
    element_store = NodeStore(
        storage_path=os.path.join(tmp_dir, "elements"), verbose=VERBOSE
    )
    element_store.create_nodes(element())
    return element_store


@pytest.fixture
def test_data():
    nodes_1_data = [{"name": "Source1"}, {"name": "Source2"}]
    nodes_2_data = [{"name": "Target1"}, {"name": "Target2"}]
    node_1_type = "user"
    node_2_type = "item"
    edge_type = "test_edge"
    edge_data = [
        {
            "source_id": 0,
            "source_type": node_1_type,
            "target_id": 0,
            "target_type": node_2_type,
            "edge_type": edge_type,
            "weight": 0.5,
        },
        {
            "source_id": 1,
            "source_type": node_2_type,
            "target_id": 1,
            "target_type": node_1_type,
            "edge_type": edge_type,
            "weight": 0.7,
        },
        {
            "source_id": 0,
            "source_type": node_1_type,
            "target_id": 1,
            "target_type": node_2_type,
            "edge_type": edge_type,
            "weight": 0.5,
        },
        {
            "source_id": 1,
            "source_type": node_2_type,
            "target_id": 0,
            "target_type": node_1_type,
            "edge_type": edge_type,
            "weight": 0.7,
        },
    ]
    return nodes_1_data, node_1_type, nodes_2_data, node_2_type, edge_data, edge_type


@pytest.fixture
def wyckoff_generator():
    """Fixture to provide a sample node generator function."""

    def generate_wyckoff_nodes():
        """Generate basic Wyckoff position nodes."""
        data = [
            {"symbol": "1a", "multiplicity": 1, "letter": "a", "site_symmetry": "1"},
            {"symbol": "2b", "multiplicity": 2, "letter": "b", "site_symmetry": "2"},
        ]
        return pa.Table.from_pylist(data)

    return generate_wyckoff_nodes


@pytest.fixture
def node_generator_data(wyckoff_generator):
    """Fixture providing test data for node generators."""
    generator_name = "test_wyckoff_generator"
    generator_func = wyckoff_generator
    generator_args = {}
    generator_kwargs = {}
    return generator_name, generator_func, generator_args, generator_kwargs


def test_initialize_graphdb(graphdb):
    """Test if GraphDB initializes with the correct directories."""
    assert os.path.exists(graphdb.nodes_path), "Nodes directory not created."
    assert os.path.exists(graphdb.edges_path), "Edges directory not created."
    assert os.path.exists(
        graphdb.edge_generators_path
    ), "Edge generators directory not created."
    assert os.path.exists(graphdb.graph_path), "Graph directory not created."


def test_add_node_type(graphdb):
    """Test adding a node type."""
    node_type = "test_node"
    graphdb.add_node_type(node_type)
    node_store_path = os.path.join(graphdb.nodes_path, node_type)
    assert os.path.exists(node_store_path), "Node type directory not created."
    assert node_type in graphdb.node_stores, "Node type not registered in node_stores."

    store = graphdb.node_stores[node_type]
    assert isinstance(store, NodeStore), "Node store is not of type NodeStore."
    table = store.read_nodes()
    assert table.num_rows == 0, "Node store is not empty."

    store = graphdb.get_node_store(node_type)
    assert (
        store == graphdb.node_stores[node_type]
    ), "Node store not retrieved correctly."


def test_add_nodes(graphdb):
    """Test adding nodes to a node type."""
    node_type_1 = "test_node_1"
    node_type_2 = "test_node_2"
    data_1 = [{"name": "Node1"}, {"name": "Node2"}]
    data_2 = [{"name": "Node3"}, {"name": "Node4"}, {"name": "Node5"}]
    graphdb.add_nodes(node_type_1, data_1)
    graphdb.add_nodes(node_type_2, data_2)

    assert (
        node_type_1 in graphdb.node_stores
    ), "Node type 1 not registered in node_stores."
    assert (
        node_type_2 in graphdb.node_stores
    ), "Node type 2 not registered in node_stores."

    node_store_1 = graphdb.get_node_store(node_type_1)
    node_store_2 = graphdb.get_node_store(node_type_2)

    assert len(node_store_1.read_nodes()) == len(
        data_1
    ), "Incorrect number of nodes added."
    assert len(node_store_2.read_nodes()) == len(
        data_2
    ), "Incorrect number of nodes added."


def test_add_node_store(tmp_dir):
    """Test adding a node store to the graph database."""
    # Create a temporary node store
    temp_store_path = os.path.join(tmp_dir, "temp_store")
    temp_store = NodeStore(storage_path=temp_store_path, verbose=VERBOSE)

    # Add some test data to the temp store
    test_data = [{"name": "Node1"}, {"name": "Node2"}]
    temp_store.create_nodes(test_data)

    # Create a graph database
    graph = ParquetGraphDB(storage_path=os.path.join(tmp_dir, "graph"), verbose=VERBOSE)

    # Add the node store to the graph
    graph.add_node_store(temp_store)

    # Verify the store was added correctly
    assert temp_store.node_type in graph.node_stores
    assert len(graph.get_nodes(temp_store.node_type)) == 2

    # Test overwrite=False behavior
    new_store = NodeStore(
        storage_path=os.path.join(tmp_dir, "new_store"), verbose=VERBOSE
    )
    new_store.node_type = temp_store.node_type

    with pytest.raises(
        ValueError, match=f"Node store of type {temp_store.node_type} already exists"
    ):
        graph.add_node_store(new_store, overwrite=False)

    # Test overwrite=True behavior
    new_data = [{"name": "Node3"}]
    new_store.create_nodes(new_data)
    graph.add_node_store(new_store, overwrite=True)

    # Verify the store was overwritten
    assert len(graph.get_nodes(new_store.node_type)) == 1
    nodes = graph.get_nodes(new_store.node_type)
    assert nodes.to_pydict()["name"] == ["Node3"]


def test_add_node_store_with_remove_original(tmp_dir):
    """Test adding a node store with remove_original=True option."""
    # Create a temporary node store
    node_type = "test_node"
    temp_store_path = os.path.join(tmp_dir, node_type)
    temp_store = NodeStore(storage_path=temp_store_path, verbose=VERBOSE)

    # Add test data and ensure it's written to disk
    test_data = [{"name": "Node1"}]
    temp_store.create_nodes(test_data)

    # Create a graph database
    graphdb = ParquetGraphDB(
        storage_path=os.path.join(tmp_dir, "graph"), verbose=VERBOSE
    )

    # Add the node store with remove_original=True
    graphdb.add_node_store(temp_store, remove_original=True)

    # Verify original store directory was removed
    assert not os.path.exists(temp_store_path)

    # Verify data was transferred correctly
    assert temp_store.node_type in graphdb.node_stores

    # Ensure the new store location exists before trying to read
    new_store_path = os.path.join(graphdb.nodes_path, node_type)
    assert os.path.exists(new_store_path), "New store location doesn't exist"

    nodes = graphdb.get_nodes(node_type)
    assert nodes.to_pydict()["name"] == ["Node1"]


def test_nodes_persist_after_reload(tmp_dir):
    """Test that nodes persist and can be loaded after recreating the GraphDB instance."""
    # Create initial graph instance and add nodes
    graph = ParquetGraphDB(storage_path=tmp_dir, verbose=VERBOSE)
    node_type = "test_node"
    test_data = [{"name": "Node1", "value": 10}, {"name": "Node2", "value": 20}]
    graph.add_nodes(node_type, test_data)

    # Verify initial data
    initial_nodes = graph.get_nodes(node_type)
    assert len(initial_nodes) == 2, "Incorrect number of nodes added."
    assert initial_nodes.to_pydict()["name"] == [
        "Node1",
        "Node2",
    ], "Incorrect node names."

    # Create new graph instance (simulating program restart)
    new_graph = ParquetGraphDB(storage_path=tmp_dir, verbose=VERBOSE)

    # Verify data persisted
    loaded_nodes = new_graph.get_nodes(node_type)
    assert len(loaded_nodes) == 2, "Incorrect number of nodes loaded."
    loaded_dict = loaded_nodes.to_pydict()
    assert loaded_dict["name"] == ["Node1", "Node2"], "Incorrect node names."
    assert loaded_dict["value"] == [10, 20], "Incorrect node values."
    assert (
        node_type in new_graph.node_stores
    ), "Node type not registered in node_stores."


def test_node_exists(graphdb):
    """Test checking if a node type exists."""
    node_type = "test_node"

    # Initially node type should not exist
    assert not graphdb.node_exists(node_type), "Node type should not exist."

    # Add nodes and verify node type exists
    test_data = [{"name": "Node1"}]
    graphdb.add_nodes(node_type, test_data)
    assert graphdb.node_exists(node_type), "Node type should exist."

    # Non-existent node type should return False
    assert not graphdb.node_exists(
        "nonexistent_type"
    ), "Non-existent node type should not exist."


def test_node_is_empty(graphdb):
    """Test checking if a node type is empty."""
    node_type = "test_node"

    assert not graphdb.node_exists(node_type), "Node type should not exist."

    # Add nodes and verify node type is not empty
    test_data = [{"name": "Node1"}]
    graphdb.add_nodes(node_type, test_data)
    assert not graphdb.node_is_empty(node_type), "Node type should not be empty."


def test_add_edge_type(graphdb):
    """Test adding an edge type."""
    edge_type = "test_edge"
    graphdb.add_edge_type(edge_type)
    edge_store_path = os.path.join(graphdb.edges_path, edge_type)
    assert os.path.exists(edge_store_path), "Edge type directory not created."
    assert edge_type in graphdb.edge_stores, "Edge type not registered in edge_stores."

    store = graphdb.edge_stores[edge_type]
    assert isinstance(store, EdgeStore), "Edge store is not of type EdgeStore."
    table = store.read_edges()
    assert table.num_rows == 0, "Edge store is not empty."

    store = graphdb.get_edge_store(edge_type)
    assert (
        store == graphdb.edge_stores[edge_type]
    ), "Edge store not retrieved correctly."


def test_add_edges(graphdb, test_data):
    """Test adding edges."""
    # First create some nodes that the edges will connect
    nodes_1_data, node_1_type, nodes_2_data, node_2_type, edge_data, edge_type = (
        test_data
    )
    graphdb.add_nodes(node_1_type, nodes_1_data)
    graphdb.add_nodes(node_2_type, nodes_2_data)

    assert (
        node_1_type in graphdb.node_stores
    ), "Node type 1 not registered in node_stores."
    assert (
        node_2_type in graphdb.node_stores
    ), "Node type 2 not registered in node_stores."

    graphdb.add_edges(edge_type, edge_data)

    assert edge_type in graphdb.edge_stores, "Edge type not registered in edge_stores."

    edges = graphdb.read_edges(edge_type)
    assert len(edges) == 4, "Incorrect number of edges added."


def test_edge_exists_and_is_empty(graphdb, test_data):
    """Test checking if an edge type exists and is empty."""
    nodes_1_data, node_1_type, nodes_2_data, node_2_type, edge_data, edge_type = (
        test_data
    )

    logger.info(f"Test data \n{test_data}")
    graphdb.add_nodes(node_1_type, nodes_1_data)
    graphdb.add_nodes(node_2_type, nodes_2_data)

    # Initially edge type should not exist
    assert not graphdb.edge_exists(edge_type), "Edge type should not exist."

    # Add edge type and verify it exists but is empty
    graphdb.add_edge_type(edge_type)
    assert graphdb.edge_exists(edge_type), "Edge type should exist."
    assert graphdb.edge_is_empty(edge_type), "Edge type should be empty."

    # Add some edges and verify it's no longer empty
    graphdb.add_edges(edge_type, edge_data)
    assert not graphdb.edge_is_empty(edge_type), "Edge type should not be empty."


def test_remove_edge_store(graphdb, test_data):
    """Test removing an edge store."""
    nodes_1_data, node_1_type, nodes_2_data, node_2_type, edge_data, edge_type = (
        test_data
    )
    graphdb.add_nodes(node_1_type, nodes_1_data)
    graphdb.add_nodes(node_2_type, nodes_2_data)

    # Add an edge type
    graphdb.add_edge_type(edge_type)
    assert edge_type in graphdb.edge_stores, "Edge type not added correctly."

    # Remove the edge store
    graphdb.remove_edge_store(edge_type)
    assert (
        edge_type not in graphdb.edge_stores
    ), "Edge type not removed from edge_stores."
    assert not os.path.exists(
        os.path.join(graphdb.edges_path, edge_type)
    ), "Edge store directory not removed."

    # Verify that trying to get the removed store raises an error
    with pytest.raises(
        ValueError, match=f"Edge store of type {edge_type} does not exist"
    ):
        graphdb.get_edge_store(edge_type)


def test_edges_persist_after_reload(tmp_dir, test_data):
    """Test that edges persist and can be loaded after recreating the GraphDB instance."""
    # Create initial graph instance and add edges
    graph = ParquetGraphDB(storage_path=tmp_dir, verbose=VERBOSE)
    nodes_1_data, node_1_type, nodes_2_data, node_2_type, edge_data, edge_type = (
        test_data
    )

    # nodes_1_data = test_data[0]
    # node_1_type = test_data[1]
    # nodes_2_data = test_data[2]
    # node_2_type = test_data[3]
    # edge_data = test_data[4]
    # edge_type = test_data[5]

    graph.add_nodes(node_1_type, nodes_1_data)
    graph.add_nodes(node_2_type, nodes_2_data)
    graph.add_edges(edge_type, edge_data)

    # Create new graph instance (simulating program restart)

    new_graph = ParquetGraphDB(storage_path=tmp_dir, verbose=VERBOSE)

    # Verify edges persisted
    assert edge_type in new_graph.edge_stores, "Edge type not loaded."
    edges = new_graph.read_edges(edge_type)
    assert len(edges) == 4, "Incorrect number of edges loaded."
    edge_dict = edges.to_pydict()
    assert edge_dict["weight"] == [0.5, 0.7, 0.5, 0.7], "Incorrect edge weights."


def test_add_edge_generator(graphdb, element_store):
    """Test adding an edge generator to the GraphDB."""
    # Get the generator name from the function

    generator_name = element_element_neighborsByGroupPeriod.__name__

    graphdb.add_node_store(element_store)
    # Add the generator
    graphdb.add_edge_generator(
        element_element_neighborsByGroupPeriod,
        generator_args={"element_store": element_store},
    )

    # Verify the generator was added
    assert graphdb.edge_generator_store.is_in(generator_name)


def test_run_edge_generator(graphdb, element_store):
    """Test running an edge generator and verify its output."""

    graphdb.add_node_store(element_store)

    generator_name = element_element_neighborsByGroupPeriod.__name__

    # Add and run the generator
    graphdb.add_edge_generator(
        element_element_neighborsByGroupPeriod,
        generator_args={"element_store": element_store},
    )

    table = graphdb.run_edge_generator(generator_name)

    # Verify the output table has the expected structure
    assert isinstance(table, pa.Table)
    expected_columns = {
        "source_id",
        "target_id",
        "source_type",
        "target_type",
        "edge_type",
        "weight",
        "source_name",
        "target_name",
        "name",
        "source_extended_group",
        "source_period",
        "target_extended_group",
        "target_period",
    }
    assert set(table.column_names) == expected_columns

    # Convert to pandas for easier verification
    df = table.to_pandas()

    # Basic validation checks
    assert not df.empty, "Generator produced no edges"
    assert all(df["source_type"] == "elements"), "Incorrect source_type"
    assert all(df["target_type"] == "elements"), "Incorrect target_type"
    assert all(df["weight"] == 1.0), "Incorrect weight values"

    # Verify edge names are properly formatted
    assert all(
        df["name"].str.contains("_neighborsByGroupPeriod_")
    ), "Edge names not properly formatted"


def test_edge_generator_persistence(tmp_dir, element_store):
    """Test that edge generators persist when reloading the GraphDB."""
    generator_name = element_element_neighborsByGroupPeriod.__name__

    # Create initial graph instance and add generator
    graph = ParquetGraphDB(storage_path=tmp_dir, verbose=VERBOSE)
    graph.add_node_store(element_store)
    graph.add_edge_generator(
        element_element_neighborsByGroupPeriod,
        generator_args={"element_store": element_store},
    )

    # Create new graph instance (simulating program restart)
    new_graph = ParquetGraphDB(storage_path=tmp_dir, verbose=VERBOSE)

    # Verify generator was loaded
    assert new_graph.edge_generator_store.is_in(generator_name)

    # Verify generator still works
    table = new_graph.run_edge_generator(generator_name)
    assert isinstance(table, pa.Table), "Generator output is not a pyarrow Table"
    assert len(table) > 0, "Generator produced no edges"

    assert table.shape == (
        391,
        13,
    ), f"Generator for element_element_neighborsByGroupPeriod edge output shape ({table.shape}) is incorrect"


def test_invalid_generator_args(graphdb, element_store):
    """Test that invalid generator arguments raise appropriate errors."""

    # element_store = ElementNodes(storage_path=os.path.join(tmp_dir, 'elements'))
    generator_name = element_element_neighborsByGroupPeriod.__name__
    graphdb.add_node_store(element_store)

    # Test missing required argument
    with pytest.raises(Exception):
        graphdb.add_edge_generator(
            element_element_neighborsByGroupPeriod,
            generator_args={},  # Missing element_store
            run_immediately=False,
        )
        graphdb.run_edge_generator(generator_name)

    # Test invalid element_store argument
    with pytest.raises(Exception):
        graphdb.add_edge_generator(
            element_element_neighborsByGroupPeriod,
            generator_args={"element_store": "invalid_store"},
        )


def test_add_node_generator(graphdb, wyckoff_generator):
    """Test adding a node generator to the GraphDB."""

    generator_name = wyckoff_generator.__name__
    # Add the generator
    graphdb.add_node_generator(wyckoff_generator)

    # Verify the generator was added
    assert graphdb.node_generator_store.is_in(generator_name)

    wyckoff_node_store = graphdb.get_node_store(generator_name)

    nodes = wyckoff_node_store.read_nodes()
    nodes_df = nodes.to_pandas()

    assert nodes_df.shape == (2, 5)


def test_run_node_generator(graphdb, wyckoff_generator):
    """Test running a node generator and verify its output."""
    generator_name = wyckoff_generator.__name__

    # Add and run the generator
    graphdb.add_node_generator(
        wyckoff_generator,
        generator_args=None,
        generator_kwargs=None,
        run_immediately=False,
    )

    graphdb.run_node_generator(generator_name)

    wyckoff_node_store = graphdb.get_node_store(generator_name)

    nodes = wyckoff_node_store.read_nodes()
    nodes_df = nodes.to_pandas()

    # Verify the output table has the expected structure
    assert isinstance(nodes_df, pd.DataFrame)
    expected_columns = {"symbol", "multiplicity", "letter", "site_symmetry", "id"}
    assert set(nodes_df.columns) == expected_columns

    # Basic validation checks
    assert not nodes_df.empty, "Generator produced no nodes"
    assert nodes_df.shape == (2, 5), "Generator has the wrong shape"


def test_node_generator_persistence(tmp_dir, wyckoff_generator):
    """Test that node generators persist when reloading the GraphDB."""
    generator_name = wyckoff_generator.__name__
    # Create initial graph instance and add generator
    graph = ParquetGraphDB(storage_path=tmp_dir, verbose=VERBOSE)
    graph.add_node_generator(
        wyckoff_generator,
        run_immediately=False,
    )

    # Create new graph instance (simulating program restart)
    new_graph = ParquetGraphDB(storage_path=tmp_dir, verbose=VERBOSE)

    # Verify generator was loaded
    assert new_graph.node_generator_store.is_in(generator_name)

    # Verify generator still works
    new_graph.run_node_generator(generator_name)

    wyckoff_node_store = new_graph.get_node_store(generator_name)

    nodes = wyckoff_node_store.read_nodes()
    nodes_df = nodes.to_pandas()

    assert isinstance(
        nodes_df, pd.DataFrame
    ), "Generator output is not a pandas DataFrame"
    assert len(nodes_df) > 0, "Generator produced no nodes"
    assert nodes_df.shape == (2, 5), "Generator output shape is incorrect"


def test_invalid_node_generator_args(graphdb):
    """Test that invalid node generator arguments raise appropriate errors."""

    def bad_generator(nonexistent_arg):
        return pa.Table.from_pylist([{"test": 1}])

    with pytest.raises(Exception):
        graphdb.add_node_generator(
            bad_generator,
        )
        graphdb.run_node_generator("bad_generator")
