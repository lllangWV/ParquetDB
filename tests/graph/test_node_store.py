import logging
import os
import shutil
import warnings

import pandas as pd
import pyarrow as pa
import pytest

from parquetdb.graph import NodeStore

logger = logging.getLogger(__name__)
VERBOSE = 2


@pytest.fixture
def temp_storage(tmp_path):
    """Fixture to create and cleanup a temporary storage directory"""
    storage_dir = tmp_path / "test_node_store"
    yield str(storage_dir)
    if os.path.exists(storage_dir):
        shutil.rmtree(storage_dir)


@pytest.fixture
def node_store(temp_storage):
    """Fixture to create a NodeStore instance"""
    return NodeStore(temp_storage, verbose=VERBOSE)


def test_node_store_initialization(temp_storage):
    """Test that NodeStore initializes correctly and creates the storage directory"""
    store = NodeStore(temp_storage, verbose=VERBOSE)
    assert os.path.exists(temp_storage)
    assert store is not None


def test_create_nodes_from_dict(node_store):
    """Test creating nodes from a dictionary"""
    test_data = {"name": ["node1", "node2"], "value": [1.0, 2.0]}
    node_store.create_nodes(test_data)

    # Read back and verify
    result_table = node_store.read_nodes()
    result_df = result_table.to_pandas()
    assert len(result_df) == 2
    assert "name" in result_df.columns
    assert "value" in result_df.columns
    assert list(result_df["name"]) == ["node1", "node2"]
    assert list(result_df["value"]) == [1.0, 2.0]


def test_create_nodes_from_dataframe(node_store):
    """Test creating nodes from a pandas DataFrame"""
    df = pd.DataFrame({"name": ["node1", "node2"], "value": [1.0, 2.0]})
    node_store.create_nodes(df)

    result_table = node_store.read_nodes()
    result_df = result_table.to_pandas()
    assert len(result_df) == 2
    assert all(result_df["name"] == df["name"])
    assert all(result_df["value"] == df["value"])


def test_read_nodes_with_filters(node_store):
    """Test reading nodes with specific filters"""
    test_data = {"name": ["node1", "node2", "node3"], "value": [1.0, 2.0, 3.0]}
    node_store.create_nodes(test_data)

    # Read with column filter
    result_table = node_store.read_nodes(columns=["id", "name"])
    result_df = result_table.to_pandas()
    assert list(result_df.columns) == ["id", "name"]

    # Read with ID filter
    first_result_table = node_store.read_nodes()
    first_result_df = first_result_table.to_pandas()
    first_id = first_result_df["id"].iloc[0]
    filtered_result_table = node_store.read_nodes(ids=[first_id])
    filtered_result_df = filtered_result_table.to_pandas()

    assert len(filtered_result_df) == 1
    assert filtered_result_df["id"].iloc[0] == first_id


def test_update_nodes(node_store):
    """Test updating existing nodes"""
    # Create initial data
    initial_data = {"name": ["node1", "node2"], "value": [1.0, 2.0]}
    node_store.create_nodes(initial_data)

    # Get the IDs
    existing_nodes_table = node_store.read_nodes()
    existing_nodes_df = existing_nodes_table.to_pandas()
    first_id = existing_nodes_df["id"].iloc[0]

    # Update the first node
    update_data = {"id": [first_id], "value": [10.0]}
    node_store.update_nodes(update_data)

    # Verify update
    updated_nodes_table = node_store.read_nodes()
    updated_nodes_df = updated_nodes_table.to_pandas()
    assert updated_nodes_df[updated_nodes_df["id"] == first_id]["value"].iloc[0] == 10.0


def test_delete_nodes(node_store):
    """Test deleting nodes"""
    # Create initial data
    initial_data = {"name": ["node1", "node2", "node3"], "value": [1.0, 2.0, 3.0]}
    node_store.create_nodes(initial_data)

    # Get the IDs
    existing_nodes_table = node_store.read_nodes()
    existing_nodes_df = existing_nodes_table.to_pandas()
    first_id = existing_nodes_df["id"].iloc[0]

    # Delete one node
    node_store.delete_nodes(ids=[first_id])

    # Verify deletion
    remaining_nodes_table = node_store.read_nodes()
    remaining_nodes_df = remaining_nodes_table.to_pandas()
    assert len(remaining_nodes_df) == 2
    assert first_id not in remaining_nodes_df["id"].values


def test_delete_columns(node_store):
    """Test deleting specific columns"""
    test_data = {"name": ["node1", "node2"], "value1": [1.0, 2.0], "value2": [3.0, 4.0]}
    node_store.create_nodes(test_data)

    # Delete one column
    node_store.delete_nodes(columns=["value2"])

    # Verify column deletion
    result_table = node_store.read_nodes()
    result_df = result_table.to_pandas()
    assert "value2" not in result_df.columns
    assert "value1" in result_df.columns
    assert "name" in result_df.columns


def test_create_nodes_with_schema(node_store):
    """Test creating nodes with a specific schema"""
    schema = pa.schema([("name", pa.string()), ("value", pa.float64())])

    test_data = {"name": ["node1", "node2"], "value": [1.0, 2.0]}

    node_store.create_nodes(test_data, schema=schema)
    result_table = node_store.read_nodes()
    result_df = result_table.to_pandas()
    assert len(result_df) == 2
    assert result_df["value"].dtype == "float64"


def test_normalize_nodes(node_store):
    """Test the normalize operation"""
    test_data = {"name": ["node1", "node2"], "value": [1.0, 2.0]}
    node_store.create_nodes(test_data)

    # This should not raise any errors
    node_store.normalize_nodes()

    # Verify data is still accessible after normalization
    result_table = node_store.read_nodes()
    result_df = result_table.to_pandas()
    assert len(result_df) == 2


def test_set_node_type(node_store):
    """Test the set_node_type operation"""
    node_store.set_node_type("new_node_type")
    assert node_store.node_type == "new_node_type"

    node_store.node_type = "new_node_type_2"
    assert node_store.node_type == "new_node_type_2"
