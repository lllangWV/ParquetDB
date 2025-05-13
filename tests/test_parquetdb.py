import json
import logging
import os
import shutil
import tempfile
import time
import unittest

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from parquetdb import ParquetDB, config
from parquetdb.core import types
from parquetdb.core.parquetdb import LoadConfig, NormalizeConfig
from parquetdb.utils import pyarrow_utils

logger = logging.getLogger("tests")

VERBOSE = 1
with open(os.path.join(config.tests_dir, "data", "alexandria_test.json"), "r") as f:
    alexandria_data = json.load(f)

pytestmark = pytest.mark.filterwarnings(
    "ignore:__array__ implementation doesn't accept a copy keyword.*:DeprecationWarning"
)


TEMP_DIR = tempfile.mkdtemp()


class TestParquetDB(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory for the database
        self.temp_dir = os.path.join(TEMP_DIR, "temp-1")
        self.temp_dir_2 = os.path.join(TEMP_DIR, "temp-2")

        self.db = ParquetDB(
            db_path=os.path.join(self.temp_dir, "test_db"), verbose=VERBOSE
        )

        # Create some test data
        self.test_data = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35},
        ]
        logger.debug(f"Test data: {self.test_data}")
        self.test_df = pd.DataFrame(self.test_data)

    def tearDown(self):
        # Remove the temporary directory after the test
        try:
            if os.path.exists(self.temp_dir):
                shutil.rmtree(self.temp_dir)

            if os.path.exists(self.temp_dir_2):
                shutil.rmtree(self.temp_dir_2)
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
        # For some reason, there are race conditions when
        # deleting the directory and performaing another test
        # time.sleep(0.1)

    def test_create_and_read(self):
        logger.info("Testing create and read")
        # Test creating data and reading it back
        data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        self.db.create(data)
        table = self.db.read()
        df = table.to_pandas()
        logger.debug(f"DataFrame:\n{df}")
        data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        self.db.create(data)

        # Read the data back
        table = self.db.read()
        df = table.to_pandas()
        logger.debug(f"DataFrame:\n{df}")

        # Assertions
        self.assertEqual(len(df), 4)
        self.assertIn("name", df.columns)
        self.assertIn("age", df.columns)
        self.assertEqual(df[df["age"] == 30].iloc[0]["name"], "Alice")
        self.assertEqual(df[df["age"] == 25].iloc[0]["name"], "Bob")

        logger.info("Test create and read passed")

    def test_update(self):
        logger.info("Testing update")
        # Test updating existing records

        data = [{"name": "Charlie", "age": 28}, {"name": "Diana", "age": 32}]
        self.db.create(data)

        # Read the data back
        result = self.db.read()
        df = result.to_pandas()
        logger.debug(f"DataFrame:\n{df}")

        # Update the age of 'Charlie'
        update_data = [{"id": 0, "age": 29}]
        self.db.update(update_data)

        # Read the data back
        result = self.db.read()
        df = result.to_pandas()
        logger.debug(f"DataFrame:\n{df}")

        # Assertions
        self.assertEqual(df[df["name"] == "Charlie"].iloc[0]["age"], 29)
        self.assertEqual(df[df["name"] == "Diana"].iloc[0]["age"], 32)
        logger.info("Test update passed")

    def test_delete(self):
        # Test deleting records
        data = [{"name": "Eve", "age": 35}, {"name": "Frank", "age": 40}]
        self.db.create(data)

        # Delete 'Eve'
        self.db.delete(
            ids=[0],
        )

        # Read the data back
        result = self.db.read()
        df = result.to_pandas()

        # Assertions
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["name"], "Frank")

    def test_create_and_normalize(self):
        # Step 1: Create data without normalization
        self.db.create(data=self.test_df, normalize_dataset=False)

        # Step 2: Verify that data has been written to the dataset directory
        dataset_files = self.db.get_current_files()
        self.assertGreater(
            len(dataset_files),
            0,
            "No parquet files found after create without normalization.",
        )

        # Load the data to check its state before normalization
        loaded_data = self.db.read()
        self.assertEqual(
            loaded_data.num_rows,
            len(self.test_data),
            "Mismatch in row count before normalization.",
        )

        # Step 3: Run normalization. Will normalize to 1 row per file and 1 row per group
        self.db.normalize(
            normalize_config=NormalizeConfig(
                max_rows_per_file=1, min_rows_per_group=1, max_rows_per_group=1
            )
        )

        # Step 4: Verify that the data has been normalized (e.g., consistent row distribution)
        normalized_data = self.db.read()
        self.assertEqual(
            normalized_data.num_rows, 3, "Mismatch in row count after normalization."
        )

        # Additional checks to ensure normalization affects file structure (if applicable)
        normalized_files = self.db.get_current_files()
        self.assertGreaterEqual(
            len(normalized_files), 3, "No files found after normalization."
        )

    def test_filters(self):
        # Test reading data with filters
        data = [
            {"name": "Grace", "age": 22},
            {"name": "Heidi", "age": 27},
            {"name": "Ivan", "age": 35},
        ]
        self.db.create(data)

        # Apply filter to get people older than 25
        age_filter = pc.field("age") > 25
        result = self.db.read(filters=[age_filter])
        df = result.to_pandas()

        # Assertions
        self.assertEqual(len(df), 2)
        self.assertListEqual(df["name"].tolist(), ["Heidi", "Ivan"])

    def test_add_new_field(self):
        # Test adding data with a new field and ensure schema evolves
        # This also test that if new incoming data doesn't have a
        # field that is already in the schema, a null value of the correct type is added
        data = [{"name": "Judy", "age": 29}]
        self.db.create(data)

        # Add new data with an additional field
        new_data = [{"name": "Karl", "occupation": "Engineer"}]
        self.db.create(new_data)

        # Read back the data
        table = self.db.read()
        df = table.to_pandas()

        # Assertions
        self.assertIn("occupation", df.columns)
        self.assertEqual(df[df["name"] == "Karl"].iloc[0]["occupation"], "Engineer")
        self.assertTrue(pd.isnull(df[df["name"] == "Judy"].iloc[0]["occupation"]))
        self.assertTrue(np.isnan(df[df["name"] == "Karl"].iloc[0]["age"]))

    def test_nested_data_handling(self):

        py_lattice_1 = [
            [[1.1, 0, 0], [0, 1, 0], [0, 0, 1]],
            [[1, 0, 0], [0, 1, 0], [0, 0, 1]],
        ]
        py_lattice_2 = [
            [[2.1, 0, 0], [0, 2, 0], [0, 0, 2]],
            [[2, 0, 0], [0, 2, 0], [0, 0, 2]],
        ]

        current_data = [
            {
                "b": {"x": 10, "y": 20},
                "c": {},
                "e": 5,
                "lattice": py_lattice_1,
                "unordered_list_string": ["hi", "reree"],
                "unordered_list_int": [0, 1, 3],
            },
            {
                "b": {"x": 30, "y": 40},
                "c": {},
                "lattice": py_lattice_2,
                "unordered_list_string": ["reree"],
                "unordered_list_int": [0, 1],
            },
            {"b": {"x": 30, "y": 40}, "c": {}},
            {"b": {"x": 30, "y": 40}, "c": {}},
            {"b": {"x": 30, "y": 40}, "c": {}},
            {
                "b": {"x": 30, "y": 40},
                "c": {},
                "lattice": py_lattice_2,
                "unordered_list_string": ["reree"],
                "unordered_list_int": [0, 1],
            },
        ]

        incoming_data = [
            {"id": 2, "b": {"x": 10, "y": 20}, "c": {}},
            {
                "id": 0,
                "b": {"y": 50, "z": {"a": 1.1, "b": 3.3}},
                "c": {},
                "d": 1,
                "e": 6,
                "lattice": py_lattice_2,
                "unordered_list_string": ["updated"],
            },
            {
                "id": 3,
                "b": {"y": 50, "z": {"a": 4.5, "b": 5.3}},
                "c": {},
                "d": 1,
                "e": 6,
                "lattice": py_lattice_2,
                "unordered_list_string": ["updated"],
            },
        ]

        self.db.create(current_data)

        # Initial read before the update
        table = self.db.read()
        df = table.to_pandas()

        # logger.debug(f"DataFrame:\n{df.head()}")
        # Assert the shape before the update (confirming initial data structure)
        assert df.shape[0] == len(
            current_data
        ), f"Expected {len(current_data)} rows before update, but got {df.shape[0]}"
        assert df.loc[0, "b.x"] == 10
        assert df.loc[0, "b.y"] == 20
        assert np.isnan(df.loc[0, "c.dummy_field"])
        assert df.loc[0, "e"] == 5
        assert np.array_equal(
            table["lattice"].combine_chunks().to_numpy_ndarray()[0],
            np.array(py_lattice_1),
        ), "Row 0 lattice data mismatch after update"
        assert (
            df.loc[0, "unordered_list_string"][0] == "hi"
        ), "Row 0 'unordered_list_string' did not update correctly"
        assert (
            df.loc[0, "unordered_list_string"][1] == "reree"
        ), "Row 0 'unordered_list_string' did not update correctly"
        assert (
            df.loc[0, "unordered_list_int"][0] == 0
        ), "Row 0 'unordered_list_int' did not update correctly"
        assert (
            df.loc[0, "unordered_list_int"][1] == 1
        ), "Row 0 'unordered_list_int' did not update correctly"
        assert (
            df.loc[0, "unordered_list_int"][2] == 3
        ), "Row 0 'unordered_list_int' did not update correctly"

        assert df.loc[1, "b.x"] == 30
        assert df.loc[1, "b.y"] == 40
        assert np.isnan(df.loc[1, "c.dummy_field"])
        assert np.isnan(df.loc[1, "e"])
        assert np.array_equal(
            table["lattice"].combine_chunks().to_numpy_ndarray()[1],
            np.array(py_lattice_2),
        ), "Row 0 lattice data mismatch after update"
        assert (
            df.loc[1, "unordered_list_string"][0] == "reree"
        ), "Row 0 'unordered_list_string' did not update correctly"
        assert (
            df.loc[1, "unordered_list_int"][0] == 0
        ), "Row 0 'unordered_list_int' did not update correctly"
        assert (
            df.loc[1, "unordered_list_int"][1] == 1
        ), "Row 0 'unordered_list_int' did not update correctly"

        assert df.loc[2, "b.x"] == 30
        assert df.loc[2, "b.y"] == 40
        assert np.isnan(df.loc[2, "c.dummy_field"])
        assert np.isnan(df.loc[2, "e"])
        assert np.isnan(df.loc[2, "e"])
        assert np.isnan(df.loc[2, "e"])
        assert np.isnan(df.loc[2, "lattice"]).all()
        assert df.loc[2, "unordered_list_string"] == None
        assert df.loc[2, "unordered_list_int"] == None

        assert df.shape[0] == len(
            current_data
        ), f"Expected {len(current_data)} rows before update, but got {df.shape[0]}"
        assert df.loc[5, "b.x"] == 30
        assert df.loc[5, "b.y"] == 40
        assert np.isnan(df.loc[5, "c.dummy_field"])
        assert np.isnan(df.loc[5, "e"])
        assert np.array_equal(
            table["lattice"].combine_chunks().to_numpy_ndarray()[5],
            np.array(py_lattice_2),
        ), "Row 0 lattice data mismatch after update"
        assert (
            df.loc[5, "unordered_list_string"][0] == "reree"
        ), "Row 0 'unordered_list_string' did not update correctly"
        assert (
            df.loc[5, "unordered_list_int"][0] == 0
        ), "Row 0 'unordered_list_int' did not update correctly"
        assert (
            df.loc[5, "unordered_list_int"][1] == 1
        ), "Row 0 'unordered_list_int' did not update correctly"

        # Perform the update
        self.db.update(incoming_data)

        # Read back the data after the update
        table = self.db.read()
        df = table.to_pandas()

        # Assert that the number of rows remains the same (no new rows added)
        assert df.shape[0] == len(
            current_data
        ), f"Expected {len(current_data)} rows after update, but got {df.shape[0]}"

        # Check if the updates for `id:0` have been applied correctly
        assert df.loc[0, "b.y"] == 50
        assert df.loc[0, "b.x"] == 10
        assert df.loc[0, "b.z.a"] == 1.1
        assert df.loc[0, "b.z.b"] == 3.3
        assert df.loc[0, "e"] == 6, "Row 0 column 'e' values mismatch after update"
        assert df.loc[0, "unordered_list_string"] == [
            "updated"
        ], "Row 0 'unordered_list_string' did not update correctly"

        assert np.array_equal(
            table["lattice"].combine_chunks().to_numpy_ndarray()[0],
            np.array(py_lattice_2),
        ), "Row 0 lattice data mismatch after update"

        # Check if the updates for `id:3` have been applied correctly
        assert df.loc[3, "b.y"] == 50
        assert df.loc[3, "b.x"] == 30
        assert df.loc[3, "b.z.a"] == 4.5
        assert df.loc[3, "b.z.b"] == 5.3
        assert df.loc[3, "e"] == 6, "Row 3 column 'e' values mismatch after update"
        assert df.loc[3, "unordered_list_string"] == [
            "updated"
        ], "Row 3 'unordered_list_string' did not update correctly"
        assert np.array_equal(
            table["lattice"].combine_chunks().to_numpy_ndarray()[3],
            np.array(py_lattice_2),
        ), "Row 3 lattice data mismatch after update"

        # Check that rows without 'id' in incoming data remain unchanged (e.g., row 1)
        assert df.loc[1, "b.y"] == 40
        assert df.loc[1, "b.x"] == 30
        assert df.loc[1, "unordered_list_string"] == [
            "reree"
        ], "Row 1 'unordered_list_string' unexpectedly changed"
        assert np.array_equal(
            table["lattice"].combine_chunks().to_numpy_ndarray()[1],
            np.array(py_lattice_2),
        ), "Row 1 lattice data unexpectedly changed"

    def test_get_schema(self):
        # Test retrieving the schema
        data = [{"name": "Liam", "age": 45}]
        self.db.create(data)
        schema = self.db.get_schema()

        # Assertions
        self.assertIn("name", schema.names)
        self.assertIn("age", schema.names)
        self.assertIn("id", schema.names)

    def test_read_specific_columns(self):
        # Test reading specific columns
        data = [
            {"name": "Mia", "age": 30, "city": "New York"},
            {"name": "Noah", "age": 35, "city": "San Francisco"},
        ]
        self.db.create(data)

        # Read only the 'name' column
        result = self.db.read(columns=["name"])
        df = result.to_pandas()

        # Assertions
        self.assertEqual(len(df.columns), 1)
        self.assertIn("name", df.columns)
        self.assertNotIn("age", df.columns)
        self.assertNotIn("city", df.columns)

    def test_batch_reading(self):
        # Test reading data in batches
        data = [{"name": f"Person {i}", "age": i} for i in range(100)]
        self.db.create(data)

        # Read data in batches of 20
        batches = self.db.read(batch_size=20, load_format="batches")

        # Assertions
        batch_count = 0
        total_rows = 0
        for batch in batches:
            batch_count += 1
            total_rows += batch.num_rows
            self.assertLessEqual(batch.num_rows, 20)
        self.assertEqual(batch_count, 5)
        self.assertEqual(total_rows, 100)

    def test_update_schema(self):
        # Test updating the schema of the table
        data = [{"name": "Olivia", "age": 29}]
        self.db.create(data)

        # Read back the data
        result = self.db.read()
        df = result.to_pandas()

        logger.debug(f"DataFrame:\n{df}")

        # Update the 'age' field to be a float instead of int
        new_field = pa.field("age", pa.float64())
        field_dict = {"age": new_field}
        self.db.update_schema(field_dict=field_dict)

        # Read back the data
        result = self.db.read()
        df = result.to_pandas()

        logger.debug(f"DataFrame:\n{df}")

        # Assertions
        self.assertEqual(df["age"].dtype, "float64")

    def test_update_with_new_field_included(self):
        # Test updating the schema of the table
        data = [
            {"name": "Mia", "age": 30, "city": "New York"},
            {"name": "Noah", "age": 35, "city": "San Francisco"},
        ]
        self.db.create(data)

        # Update the 'Mia' record to include a new field and change age to 60
        data = [{"id": 0, "age": 60, "state": "NY"}]
        self.db.update(data)

        # Read back the data
        result = self.db.read()
        df = result.to_pandas()

        # Assertions
        self.assertEqual(df.iloc[0]["state"], "NY")
        self.assertEqual(df.iloc[1]["state"], None)
        self.assertEqual(df.iloc[0]["age"], 60)
        self.assertEqual(df.iloc[1]["age"], 35)

    def test_delete_nonexistent_id(self):
        # Test deleting an ID that doesn't exist
        data = [{"name": "Peter", "age": 50}]
        self.db.create(data)

        # Attempt to delete a non-existent ID
        self.db.delete(ids=[999])

        # Read back the data
        result = self.db.read()
        df = result.to_pandas()

        # Assertions
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["name"], "Peter")

    def test_metadata(self):
        metadata = self.db.get_metadata()
        self.db.set_metadata({"class": "test"})
        assert self.db.get_metadata()["class"] == "test"

        self.db.create(data=self.test_data)

        metadata = self.db.get_metadata()
        assert metadata == {"class": "test"}

        self.db.create(
            data=self.test_data, metadata={"key1": "value1", "key2": "value2"}
        )
        # Should return metadata dictionary (can be empty)
        metadata = self.db.get_metadata()

        self.assertEqual(metadata["key1"], "value1")
        self.assertEqual(metadata["key2"], "value2")

        self.db.set_metadata({"key3": "value3", "key4": "value4"})
        metadata = self.db.get_metadata()

        self.assertEqual(metadata["class"], "test")
        self.assertEqual(metadata["key1"], "value1")
        self.assertEqual(metadata["key2"], "value2")
        self.assertEqual(metadata["key3"], "value3")
        self.assertEqual(metadata["key4"], "value4")

        # Testing set_metadata with update=False
        self.db.set_metadata({"key5": "value5", "key6": "value6"}, update=False)
        assert self.db.get_metadata() == {"key5": "value5", "key6": "value6"}

        # Testing set_field_metadata with update=True

        fields_metadata = {"name": {"key1": "value1", "key2": "value2"}}
        self.db.set_field_metadata(fields_metadata=fields_metadata)
        fields_metadata = self.db.get_field_metadata(field_names=["name"])
        assert fields_metadata["name"] == {"key1": "value1", "key2": "value2"}

        # Testing set_field_metadata with update=True with existing metadata
        fields_metadata = {"name": {"key3": "value3", "key4": "value4"}}
        self.db.set_field_metadata(fields_metadata=fields_metadata, update=True)
        fields_metadata = self.db.get_field_metadata(
            field_names=["name"], return_bytes=True
        )
        assert fields_metadata["name"] == {
            b"key1": b"value1",
            b"key2": b"value2",
            b"key3": b"value3",
            b"key4": b"value4",
        }

        # Testing set_field_metadata with update=False
        fields_metadata = {"name": {"key3": "value3", "key4": "value4"}}
        self.db.set_field_metadata(fields_metadata=fields_metadata, update=False)
        fields_metadata = self.db.get_field_metadata(
            field_names="name", return_bytes=True
        )
        assert fields_metadata["name"] == {b"key3": b"value3", b"key4": b"value4"}

    def test_drop_dataset(self):
        self.db.create(data=self.test_data)
        # Drop the table and check if it no longer exists
        self.db.drop_dataset()

    def test_rename_dataset(self):
        self.db.create(data=self.test_data)
        # Rename the table and check if the new name exists
        self.db.rename_dataset("renamed_table", remove_dest=True)

        assert self.db.dataset_name == "renamed_table"

    def test_export_dataset(self):
        self.db.create(data=self.test_data)
        # Export the table to CSV
        export_path = os.path.join(self.temp_dir, "exported_table.csv")
        self.db.export_dataset(export_path, format="csv")
        self.assertTrue(os.path.exists(export_path))

        # Verify the exported data
        exported_df = pd.read_csv(export_path)
        original_df = self.db.read().to_pandas()
        pd.testing.assert_frame_equal(original_df, exported_df)

        # Export to an unsupported format
        with self.assertRaises(ValueError):
            self.db.export_dataset(export_path, format="xlsx")

    def test_rename_fields(self):
        self.db.create(data=self.test_data)
        self.db.rename_fields({"name": "first_name"})
        schema = self.db.get_schema()
        assert "first_name" in schema.names

    def test_sort_fields(self):
        self.db.create(data=self.test_data)
        self.db.sort_fields()
        schema = self.db.get_schema()
        schema_names = schema.names

        assert schema_names[0] == "age"
        assert schema_names[1] == "id"
        assert schema_names[2] == "name"

    def test_fixed_shape_tensor(self):
        a = np.eye(3).tolist()
        data_1 = [{"a": 3}, {"a": 4}]
        data_2 = [{"2d_array": a}]
        update_data = [{"2d_array": a, "id": 0}]
        self.db.create(data_1)
        self.db.create(data_2)

        table = self.db.read()
        assert table["2d_array"].combine_chunks().to_numpy_ndarray().shape == (3, 3, 3)

        self.db.update(update_data)
        table = self.db.read()
        arrays = table["2d_array"].combine_chunks().to_numpy_ndarray()
        ids = table["id"].combine_chunks().to_pylist()

        assert ids == [0, 1, 2]

        assert np.array_equal(arrays[0], np.eye(3))
        assert np.array_equal(arrays[2], np.eye(3))

    def test_rebuild_nested(self):
        data = [{"a": 1, "b": {"c": 2, "d": 3}}, {"a": 4, "b": {"c": 5, "d": 6}}]
        self.db.create(data)
        table = self.db.read(rebuild_nested_struct=True)

        assert pa.types.is_struct(table["b"].type)
        assert table["b"].type.num_fields == 2

        assert table["b"].combine_chunks().to_pylist()[0]["c"] == 2
        assert table["b"].combine_chunks().to_pylist()[0]["d"] == 3

    def test_update_maintains_existing_extension_arrays(self):
        data_1 = [{"pbc": [1, 0, 0]}, {"pbc": [0, 1, 0]}]
        self.db.create(data_1)

        table = self.db.read(ids=[0])
        assert table["pbc"].combine_chunks().to_numpy_ndarray().tolist() == [[1, 0, 0]]
        table = self.db.read(ids=[1])
        assert table["pbc"].combine_chunks().to_numpy_ndarray().tolist() == [[0, 1, 0]]

        data_2 = [{"id": 0, "density": 5}]

        self.db.update(data_2)
        table = self.db.read(ids=[1])
        df = table.to_pandas()

        logger.debug(f"Dataframe: \n {df}")
        assert table["pbc"].combine_chunks().to_numpy_ndarray().tolist() == [[0, 1, 0]]

        table = self.db.read(ids=[0])
        assert table["pbc"].combine_chunks().to_numpy_ndarray().tolist() == [[1, 0, 0]]

    def test_update_maintains_existing_extension_arrays_batches(self):
        data_1 = [{"pbc": [1, 0, 0]}, {"pbc": [0, 1, 0]}]
        self.db.create(data_1)

        table = self.db.read(ids=[0])
        pbc_array = table["pbc"].combine_chunks().to_numpy_ndarray()
        assert pbc_array.tolist() == [[1, 0, 0]]
        assert pbc_array.shape == (1, 3)

        table = self.db.read(ids=[1])
        pbc_array = table["pbc"].combine_chunks().to_numpy_ndarray()
        assert pbc_array.tolist() == [[0, 1, 0]]
        assert pbc_array.shape == (1, 3)

        table = self.db.read()
        pbc_array = table["pbc"].combine_chunks().to_numpy_ndarray()

        assert pbc_array.shape == (2, 3)

        data_2 = [{"id": 0, "density": 5}]

        self.db.update(
            data_2,
            normalize_config=NormalizeConfig(load_format="batches", batch_size=1),
        )
        table = self.db.read()
        df = table.to_pandas()

        logger.debug(f"Dataframe: \n {df}")

        table = self.db.read(ids=[0])
        pbc_array = table["pbc"].combine_chunks().to_numpy_ndarray()
        assert pbc_array.tolist() == [[1, 0, 0]]
        assert table["density"].combine_chunks().to_pylist() == [5]
        assert pbc_array.shape == (1, 3)

        table = self.db.read(ids=[1])
        pbc_array = table["pbc"].combine_chunks().to_numpy_ndarray()

        assert pbc_array.tolist() == [[0, 1, 0]]
        assert table["density"].combine_chunks().to_pylist() == [None]
        assert pbc_array.shape == (1, 3)

        table = self.db.read()
        pbc_array = table["pbc"].combine_chunks().to_numpy_ndarray()
        assert pbc_array.shape == (2, 3)

    def test_update_on_key(self):
        data_1 = [
            {"material_id": 1, "material_name": "material_1"},
            {"material_id": 2, "material_name": "material_2"},
        ]

        self.db.create(data_1)

        table = self.db.read()
        df = table.to_pandas()

        assert df[df["material_id"] == 1]["material_name"].to_list() == ["material_1"]
        assert df[df["material_id"] == 1]["material_id"].to_list() == [1]
        assert df[df["material_id"] == 2]["material_name"].to_list() == ["material_2"]
        assert df[df["material_id"] == 2]["material_id"].to_list() == [2]

        data_2 = [{"material_id": 1, "material_name": "material_1_updated"}]

        self.db.update(data_2, update_keys="material_id")

        table = self.db.read()
        df = table.to_pandas()

        logger.debug(f"Dataframe: \n {df}")

        assert df[df["material_id"] == 1]["material_name"].to_list() == [
            "material_1_updated"
        ]
        assert df[df["material_id"] == 1]["material_id"].to_list() == [1]
        assert df[df["material_id"] == 2]["material_name"].to_list() == ["material_2"]
        assert df[df["material_id"] == 2]["material_id"].to_list() == [2]

    def test_initialize_empty_table(self):
        assert self.db.is_empty()

        files = os.listdir(self.db.db_path)
        assert len(files) == 1

        table = pq.read_table(os.path.join(self.db.db_path, files[0]))
        table_shape = table.shape
        assert table_shape == (0, 1)

        for file in files:
            os.remove(os.path.join(self.db.db_path, file))

        # Test with initial fields
        db = ParquetDB(
            self.db.db_path,
            initial_fields=[
                pa.field("source_id", pa.int64()),
                pa.field("target_id", pa.int64()),
            ],
        )
        assert db.is_empty()

        files = os.listdir(db.db_path)
        assert len(files) == 1

        table = pq.read_table(os.path.join(self.db.db_path, files[0]))
        table_shape = table.shape
        assert table_shape == (0, 3)

    def test_update_multi_keys(self):
        current_data = [
            {"id_1": 100, "id_2": 10, "field_1": "here"},
            {"id_1": 55, "id_2": 11},
            {"id_1": 33, "id_2": 12},
            {"id_1": 12, "id_2": 13},
            {"id_1": 33, "id_2": 50},
        ]

        self.db.create(current_data)

        # Create second table with salary data
        incoming_data = [
            {"id_1": 100, "id_2": 10, "field_2": "there"},
            {"id_1": 5, "id_2": 5},
            {
                "id_1": 33,
                "id_2": 13,
            },  # Note: id_1 33, id_2 13 doesn't exist in current_data
            {
                "id_1": 33,
                "id_2": 12,
                "field_2": "field_2",
                "field_3": "field_3",
            },
        ]

        incoming_table = ParquetDB.construct_table(incoming_data)

        self.db.update(incoming_table, update_keys=["id_1", "id_2"])

        table = self.db.read()
        df = table.to_pandas()
        logger.debug(f"Dataframe: \n {df}")

        assert df[(df["id_1"] == 100) & (df["id_2"] == 10)]["field_1"].to_list() == [
            "here"
        ]
        assert df[(df["id_1"] == 100) & (df["id_2"] == 10)]["field_2"].to_list() == [
            "there"
        ]
        assert df[(df["id_1"] == 100) & (df["id_2"] == 10)]["field_3"].to_list() == [
            None
        ]

        assert df[(df["id_1"] == 55) & (df["id_2"] == 11)]["field_1"].to_list() == [
            None
        ]
        assert df[(df["id_1"] == 55) & (df["id_2"] == 11)]["field_2"].to_list() == [
            None
        ]
        assert df[(df["id_1"] == 55) & (df["id_2"] == 11)]["field_3"].to_list() == [
            None
        ]

        assert df[(df["id_1"] == 33) & (df["id_2"] == 12)]["field_1"].to_list() == [
            None
        ]
        assert df[(df["id_1"] == 33) & (df["id_2"] == 12)]["field_2"].to_list() == [
            "field_2"
        ]
        assert df[(df["id_1"] == 33) & (df["id_2"] == 12)]["field_3"].to_list() == [
            "field_3"
        ]

        assert df[(df["id_1"] == 12) & (df["id_2"] == 13)]["field_1"].to_list() == [
            None
        ]
        assert df[(df["id_1"] == 12) & (df["id_2"] == 13)]["field_2"].to_list() == [
            None
        ]
        assert df[(df["id_1"] == 12) & (df["id_2"] == 13)]["field_3"].to_list() == [
            None
        ]

        assert df[(df["id_1"] == 33) & (df["id_2"] == 50)]["field_1"].to_list() == [
            None
        ]
        assert df[(df["id_1"] == 33) & (df["id_2"] == 50)]["field_2"].to_list() == [
            None
        ]
        assert df[(df["id_1"] == 33) & (df["id_2"] == 50)]["field_3"].to_list() == [
            None
        ]

        assert df[(df["id_1"] == 33) & (df["id_2"] == 13)]["field_1"].to_list() == []
        assert df[(df["id_1"] == 33) & (df["id_2"] == 13)]["field_2"].to_list() == []
        assert df[(df["id_1"] == 33) & (df["id_2"] == 13)]["field_3"].to_list() == []

    def test_table_join(self):
        # Create first table with employee data
        current_data = [
            {"id_1": 100, "id_2": 10, "field_1": "here"},
            {"id_1": 33, "id_2": 12},
            {"id_1": 12, "id_2": 13, "field_2": "field_2"},
        ]

        current_table = ParquetDB.construct_table(current_data)

        # Create second table with salary data
        incoming_data = [
            {"id_1": 100, "id_2": 10, "field_2": "there"},
            {"id_1": 5, "id_2": 5},
            {"id_1": 33, "id_2": 13},  # Note: emp_id 4 doesn't exist in employees
            {
                "id_1": 33,
                "id_2": 12,
                "field_2": "field_2",
                "field_3": "field_3",
            },  # Note: emp_id 4 doesn't exist in employees
        ]

        incoming_table = ParquetDB.construct_table(incoming_data)

        logger.info(f"Starting test on a left outer join")
        join_type = "left outer"
        left_outer_table_pyarrow = incoming_table.join(
            current_table,
            keys=["id_1", "id_2"],
            right_keys=["id_1", "id_2"],
            left_suffix="_incoming",
            right_suffix="_current",
            join_type=join_type,
        )

        column_names = left_outer_table_pyarrow.column_names
        names_sorted = sorted(column_names)
        left_outer_table_pyarrow_sorted = left_outer_table_pyarrow.select(names_sorted)

        left_outer_table = pyarrow_utils.join_tables(
            incoming_table,
            current_table,
            left_keys=["id_1", "id_2"],
            right_keys=["id_1", "id_2"],
            left_suffix="_incoming",
            right_suffix="_current",
            join_type=join_type,
        )

        column_names = left_outer_table.column_names
        names_sorted = sorted(column_names)
        left_outer_table_sorted = left_outer_table.select(names_sorted)
        left_outer_table_pyarrow_sorted = left_outer_table_pyarrow_sorted.sort_by(
            [("id_1", "ascending"), ("id_2", "ascending")]
        )
        left_outer_table_sorted = left_outer_table_sorted.sort_by(
            [("id_1", "ascending"), ("id_2", "ascending")]
        )
        for name in left_outer_table_sorted.column_names:
            custom_sort = left_outer_table_pyarrow_sorted[name].to_pylist()
            pyarrow_sort = left_outer_table_sorted[name].to_pylist()
            logger.debug(f"custom_sort: \n {custom_sort}")
            logger.debug(f"pyarrow_sort: \n {pyarrow_sort}")
            assert custom_sort == pyarrow_sort

        logger.info(f"Starting test on a right outer join")
        join_type = "right outer"
        right_outer_table_pyarrow = incoming_table.join(
            current_table,
            keys=["id_1", "id_2"],
            right_keys=["id_1", "id_2"],
            left_suffix="_incoming",
            right_suffix="_current",
            join_type=join_type,
        )

        column_names = right_outer_table_pyarrow.column_names
        names_sorted = sorted(column_names)
        right_outer_table_pyarrow_sorted = right_outer_table_pyarrow.select(
            names_sorted
        )

        right_outer_table = pyarrow_utils.join_tables(
            incoming_table,
            current_table,
            left_keys=["id_1", "id_2"],
            right_keys=["id_1", "id_2"],
            left_suffix="_incoming",
            right_suffix="_current",
            join_type=join_type,
        )

        column_names = right_outer_table.column_names
        names_sorted = sorted(column_names)
        right_outer_table_sorted = right_outer_table.select(names_sorted)
        right_outer_table_pyarrow_sorted = right_outer_table_pyarrow_sorted.sort_by(
            [("id_1", "ascending"), ("id_2", "ascending")]
        )
        right_outer_table_sorted = right_outer_table_sorted.sort_by(
            [("id_1", "ascending"), ("id_2", "ascending")]
        )
        for name in right_outer_table_sorted.column_names:
            custom_sort = right_outer_table_pyarrow_sorted[name].to_pylist()
            pyarrow_sort = right_outer_table_sorted[name].to_pylist()
            logger.debug(f"custom_sort: \n {custom_sort}")
            logger.debug(f"pyarrow_sort: \n {pyarrow_sort}")
            assert custom_sort == pyarrow_sort

        logger.info(f"Starting test on a inner join")
        join_type = "inner"
        inner_table_pyarrow = incoming_table.join(
            current_table,
            keys=["id_1", "id_2"],
            right_keys=["id_1", "id_2"],
            left_suffix="_incoming",
            right_suffix="_current",
            join_type=join_type,
        )

        column_names = inner_table_pyarrow.column_names
        names_sorted = sorted(column_names)
        inner_table_pyarrow_sorted = inner_table_pyarrow.select(names_sorted)

        inner_table = pyarrow_utils.join_tables(
            incoming_table,
            current_table,
            left_keys=["id_1", "id_2"],
            right_keys=["id_1", "id_2"],
            left_suffix="_incoming",
            right_suffix="_current",
            join_type=join_type,
        )

        column_names = inner_table.column_names
        names_sorted = sorted(column_names)
        inner_table_sorted = inner_table.select(names_sorted)
        inner_table_pyarrow_sorted = inner_table_pyarrow_sorted.sort_by(
            [("id_1", "ascending"), ("id_2", "ascending")]
        )
        inner_table_sorted = inner_table_sorted.sort_by(
            [("id_1", "ascending"), ("id_2", "ascending")]
        )
        for name in inner_table_sorted.column_names:
            custom_sort = inner_table_pyarrow_sorted[name].to_pylist()
            pyarrow_sort = inner_table_sorted[name].to_pylist()
            logger.debug(f"custom_sort: \n {custom_sort}")
            logger.debug(f"pyarrow_sort: \n {pyarrow_sort}")
            assert custom_sort == pyarrow_sort

        logger.info(f"Starting test on a right anti join")
        join_type = "right anti"
        right_anti_table_pyarrow = incoming_table.join(
            current_table,
            keys=["id_1", "id_2"],
            right_keys=["id_1", "id_2"],
            left_suffix="_incoming",
            right_suffix="_current",
            join_type=join_type,
        )

        column_names = right_anti_table_pyarrow.column_names
        names_sorted = sorted(column_names)
        right_anti_table_pyarrow_sorted = right_anti_table_pyarrow.select(names_sorted)

        right_anti_table = pyarrow_utils.join_tables(
            incoming_table,
            current_table,
            left_keys=["id_1", "id_2"],
            right_keys=["id_1", "id_2"],
            left_suffix="_incoming",
            right_suffix="_current",
            join_type=join_type,
        )

        column_names = right_anti_table.column_names
        names_sorted = sorted(column_names)
        right_anti_table_sorted = right_anti_table.select(names_sorted)
        right_anti_table_pyarrow_sorted = right_anti_table_pyarrow_sorted.sort_by(
            [("id_1", "ascending"), ("id_2", "ascending")]
        )
        right_anti_table_sorted = right_anti_table_sorted.sort_by(
            [("id_1", "ascending"), ("id_2", "ascending")]
        )
        for name in right_anti_table_sorted.column_names:
            custom_sort = right_anti_table_pyarrow_sorted[name].to_pylist()
            pyarrow_sort = right_anti_table_sorted[name].to_pylist()
            logger.debug(f"custom_sort: \n {custom_sort}")
            logger.debug(f"pyarrow_sort: \n {pyarrow_sort}")
            assert custom_sort == pyarrow_sort

        logger.info(f"Starting test on a left anti join")
        join_type = "left anti"
        left_anti_table_pyarrow = incoming_table.join(
            current_table,
            keys=["id_1", "id_2"],
            right_keys=["id_1", "id_2"],
            left_suffix="_incoming",
            right_suffix="_current",
            join_type=join_type,
        )

        column_names = left_anti_table_pyarrow.column_names
        names_sorted = sorted(column_names)
        left_anti_table_pyarrow_sorted = left_anti_table_pyarrow.select(names_sorted)

        left_anti_table = pyarrow_utils.join_tables(
            incoming_table,
            current_table,
            left_keys=["id_1", "id_2"],
            right_keys=["id_1", "id_2"],
            left_suffix="_incoming",
            right_suffix="_current",
            join_type=join_type,
        )

        column_names = left_anti_table.column_names
        names_sorted = sorted(column_names)
        left_anti_table_sorted = left_anti_table.select(names_sorted)
        left_anti_table_pyarrow_sorted = left_anti_table_pyarrow_sorted.sort_by(
            [("id_1", "ascending"), ("id_2", "ascending")]
        )
        left_anti_table_sorted = left_anti_table_sorted.sort_by(
            [("id_1", "ascending"), ("id_2", "ascending")]
        )
        for name in left_anti_table_sorted.column_names:
            custom_sort = left_anti_table_pyarrow_sorted[name].to_pylist()
            pyarrow_sort = left_anti_table_sorted[name].to_pylist()
            logger.debug(f"custom_sort: \n {custom_sort}")
            logger.debug(f"pyarrow_sort: \n {pyarrow_sort}")
            assert custom_sort == pyarrow_sort

        logger.info(f"Starting test on a left semi join")
        join_type = "left semi"
        left_semi_table_pyarrow = incoming_table.join(
            current_table,
            keys=["id_1", "id_2"],
            right_keys=["id_1", "id_2"],
            left_suffix="_incoming",
            right_suffix="_current",
            join_type=join_type,
        )

        column_names = left_semi_table_pyarrow.column_names
        names_sorted = sorted(column_names)
        left_semi_table_pyarrow_sorted = left_semi_table_pyarrow.select(names_sorted)

        left_semi_table = pyarrow_utils.join_tables(
            incoming_table,
            current_table,
            left_keys=["id_1", "id_2"],
            right_keys=["id_1", "id_2"],
            left_suffix="_incoming",
            right_suffix="_current",
            join_type=join_type,
        )

        column_names = left_semi_table.column_names
        names_sorted = sorted(column_names)
        left_semi_table_sorted = left_semi_table.select(names_sorted)
        left_semi_table_pyarrow_sorted = left_semi_table_pyarrow_sorted.sort_by(
            [("id_1", "ascending"), ("id_2", "ascending")]
        )
        left_semi_table_sorted = left_semi_table_sorted.sort_by(
            [("id_1", "ascending"), ("id_2", "ascending")]
        )
        for name in left_semi_table_sorted.column_names:
            custom_sort = left_semi_table_pyarrow_sorted[name].to_pylist()
            pyarrow_sort = left_semi_table_sorted[name].to_pylist()
            logger.debug(f"custom_sort: \n {custom_sort}")
            logger.debug(f"pyarrow_sort: \n {pyarrow_sort}")
            assert custom_sort == pyarrow_sort

        logger.info(f"Starting test on a right semi join")
        join_type = "right semi"
        right_semi_table_pyarrow = incoming_table.join(
            current_table,
            keys=["id_1", "id_2"],
            right_keys=["id_1", "id_2"],
            left_suffix="_incoming",
            right_suffix="_current",
            join_type=join_type,
        )

        column_names = right_semi_table_pyarrow.column_names
        names_sorted = sorted(column_names)
        right_semi_table_pyarrow_sorted = right_semi_table_pyarrow.select(names_sorted)

        right_semi_table = pyarrow_utils.join_tables(
            incoming_table,
            current_table,
            left_keys=["id_1", "id_2"],
            right_keys=["id_1", "id_2"],
            left_suffix="_incoming",
            right_suffix="_current",
            join_type=join_type,
        )

        column_names = right_semi_table.column_names
        names_sorted = sorted(column_names)
        right_semi_table_sorted = right_semi_table.select(names_sorted)
        right_semi_table_pyarrow_sorted = right_semi_table_pyarrow_sorted.sort_by(
            [("id_1", "ascending"), ("id_2", "ascending")]
        )
        right_semi_table_sorted = right_semi_table_sorted.sort_by(
            [("id_1", "ascending"), ("id_2", "ascending")]
        )
        for name in right_semi_table_sorted.column_names:
            custom_sort = right_semi_table_pyarrow_sorted[name].to_pylist()
            pyarrow_sort = right_semi_table_sorted[name].to_pylist()
            logger.debug(f"custom_sort: \n {custom_sort}")
            logger.debug(f"pyarrow_sort: \n {pyarrow_sort}")
            assert custom_sort == pyarrow_sort

        logger.info(f"Starting test on a full outer join")
        join_type = "full outer"
        full_outer_table_pyarrow = incoming_table.join(
            current_table,
            keys=["id_1", "id_2"],
            right_keys=["id_1", "id_2"],
            left_suffix="_incoming",
            right_suffix="_current",
            join_type=join_type,
        )

        column_names = full_outer_table_pyarrow.column_names
        names_sorted = sorted(column_names)
        full_outer_table_pyarrow_sorted = full_outer_table_pyarrow.select(names_sorted)

        full_outer_table = pyarrow_utils.join_tables(
            incoming_table,
            current_table,
            left_keys=["id_1", "id_2"],
            right_keys=["id_1", "id_2"],
            left_suffix="_incoming",
            right_suffix="_current",
            join_type=join_type,
        )

        column_names = full_outer_table.column_names
        names_sorted = sorted(column_names)
        full_outer_table_sorted = full_outer_table.select(names_sorted)
        full_outer_table_pyarrow_sorted = full_outer_table_pyarrow_sorted.sort_by(
            [("id_1", "ascending"), ("id_2", "ascending")]
        )
        full_outer_table_sorted = full_outer_table_sorted.sort_by(
            [("id_1", "ascending"), ("id_2", "ascending")]
        )
        for name in full_outer_table_sorted.column_names:
            custom_sort = full_outer_table_pyarrow_sorted[name].to_pylist()
            pyarrow_sort = full_outer_table_sorted[name].to_pylist()
            logger.debug(f"custom_sort: \n {custom_sort}")
            logger.debug(f"pyarrow_sort: \n {pyarrow_sort}")
            assert custom_sort == pyarrow_sort

    def test_python_objects(self):
        self.db._serialize_python_objects = True
        from pymatgen.core import Structure

        structure = Structure(
            lattice=[[0, 2.13, 2.13], [2.13, 0, 2.13], [2.13, 2.13, 0]],
            species=["Mg", "O"],
            coords=[[0, 0, 0], [0.5, 0.5, 0.5]],
        )
        data = [
            {
                "name": "Alice",
                "age": 30,
                "time": pd.Timestamp("20180310"),
                "structure": None,
            },
            {
                "name": "Alice",
                "age": 30,
                "time": pd.Timestamp("20180310"),
                "structure": structure,
            },
        ]
        self.db.create(data)

        table = self.db.read()
        df = table.to_pandas()

        assert df["structure"][0] is None

        self.db.create(data)

        table = self.db.read()
        df = table.to_pandas()
        assert isinstance(df["structure"][1], Structure)

        data = [
            {"id": 1, "new_field": 1, "structure": None},
            {"id": 0, "new_field": 2, "structure": structure},
        ]
        self.db.update(data)

        table = self.db.read()
        df = table.to_pandas()
        # print(df)
        # print(df["structure"][1])
        assert df["new_field"][1] == 1

        assert table["structure"].type == types.PythonObjectArrowType()

    def test_transform(self):
        self.db.create(data=self.test_data)
        self.db.transform(lambda table: table.drop_columns(columns=["age"]))

        table = self.db.read()
        assert table.column_names == ["id", "name"]

        self.db.transform(
            lambda table: table.drop_columns(columns=["name"]),
            new_db_path=self.temp_dir_2,
        )

        logger.debug(f"Directory files: \n {os.listdir(self.temp_dir_2)}")
        db = ParquetDB(self.temp_dir_2)

        logger.debug(f"Directory files: \n {os.listdir(self.temp_dir_2)}")
        table = db.read()

        assert table.column_names == ["id"]
        assert table.shape == (3, 1)

        table = self.db.read()
        assert table.column_names == ["id", "name"]
        assert table.shape == (3, 2)

    def test_filter_delete(self):

        data = [
            {"name": "John", "age": 30},
            {"name": "Jane", "age": 25},
            {"name": "Jim", "age": 35},
            {"name": "Jill", "age": 30},
        ]

        self.db.create(data)

        table = self.db.read()
        assert table.shape == (4, 3)

        self.db.delete(filters=[pc.field("age") == 30])

        table = self.db.read()
        assert table.shape == (2, 3)

        df = table.to_pandas()

        assert df.iloc[0]["name"] == "Jane"
        assert df.iloc[0]["age"] == 25
        assert df.iloc[1]["name"] == "Jim"
        assert df.iloc[1]["age"] == 35

    def test_drop_duplicates(self):
        data = [
            {"id": 0, "name": "Alice", "category": 1},
            {"id": 1, "name": "Bob", "category": 1},
            {
                "id": 2,
                "name": "Bob",
                "category": 1,
            },  # Duplicate combination of (name, category)
            {"id": 3, "name": "Charlie", "category": 2},
        ]

        table = pa.Table.from_pylist(data)

        unique_keys = ["name", "category"]

        deduplicated_table = pyarrow_utils.drop_duplicates(table, unique_keys)

        assert deduplicated_table.shape == (3, 3)

    def test_get_number_of_rows_per_file(self):
        self.db.create(data=self.test_data)
        logger.debug(self.db.get_number_of_rows_per_file())
        assert self.db.get_number_of_rows_per_file() == [3]

    def test_get_number_of_row_groups_per_file(self):
        self.db.create(data=self.test_data)
        logger.debug(self.db.get_number_of_row_groups_per_file())
        assert self.db.get_number_of_row_groups_per_file() == [1]

    def test_get_row_group_sizes_per_file(self):
        self.db.create(data=self.test_data)
        logger.debug(self.db.get_row_group_sizes_per_file())
        row_group_sizes = {"test_db_0.parquet": {0: 0.0002918243408203125}}
        assert np.isclose(
            self.db.get_row_group_sizes_per_file()["test_db_0.parquet"][0],
            row_group_sizes["test_db_0.parquet"][0],
        )

    def test_get_file_sizes(self):
        self.db.create(data=self.test_data)
        logger.debug(f"file_sizes: {self.db.get_file_sizes()}")
        assert isinstance(self.db.get_file_sizes(), dict)
        assert isinstance(self.db.get_file_sizes()["test_db_0.parquet"], float)

    def test_get_parquet_column_metadata_per_file(self):
        self.db.create(data=self.test_data)
        logger.debug(
            f"parquet_column_metadata_per_file: {self.db.get_parquet_column_metadata_per_file(as_dict=True)}"
        )
        column_metadata = self.db.get_parquet_column_metadata_per_file(as_dict=True)

        column_metadata_keys = [
            "file_offset",
            "file_path",
            "physical_type",
            "num_values",
            "statistics",
            "has_dictionary_page",
            "total_uncompressed_size",
            "total_compressed_size",
        ]

        for key in column_metadata_keys:
            assert key in column_metadata["test_db_0.parquet"][0][0]

    def test_properties(self):
        self.db.create(data=self.test_data)
        logger.debug(f"n_rows: {self.db.n_rows}")
        assert self.db.n_rows == 3

        logger.debug(f"n_files: {self.db.n_files}")
        assert self.db.n_files == 1

        logger.debug(f"n_row_groups_per_file: {self.db.n_row_groups_per_file}")
        assert self.db.n_row_groups_per_file == [1]

        logger.debug(
            f"n_rows_per_row_group_per_file: {self.db.n_rows_per_row_group_per_file}"
        )
        assert self.db.n_rows_per_row_group_per_file["test_db_0.parquet"][0] == 3

        logger.debug(
            f"serialized_metadata_size_per_file: {self.db.serialized_metadata_size_per_file}"
        )
        assert isinstance(self.db.serialized_metadata_size_per_file[0], int)

    def test_summary(self):
        self.db.create(data=self.test_data)
        tmp_str = self.db.summary(show_row_group_metadata=True, show_column_names=True)
        assert "- age\n" in tmp_str
        assert "- name\n" in tmp_str
        assert "- id\n" in tmp_str

    def test_copy_dataset(self):
        self.db.create(data=self.test_data)
        self.db.copy_dataset(os.path.join(self.db.db_path, "test_copy"))
        assert os.path.exists(os.path.join(self.db.db_path, "test_copy"))


if __name__ == "__main__":
    unittest.main()

    # unittest.TextTestRunner().run(TestParquetDB('test_nested_data_handling'))
    # unittest.TextTestRunner().run(TestParquetDB('test_update_maintains_existing_extension_arrays'))
    # unittest.TextTestRunner().run(TestParquetDB('test_update_maintains_existing_extension_arrays'))
    # unittest.TextTestRunner().run(TestParquetDB('test_update_maintains_existing_extension_arrays_batches'))
    # unittest.TextTestRunner().run(TestParquetDB('test_metadata'))
    # unittest.TextTestRunner().run(TestParquetDB('test_fixed_shape_tensor'))
    # unittest.TextTestRunner().run(TestParquetDB('test_metadata'))
    # unittest.TextTestRunner().run(TestParquetDB('test_initialize_empty_table'))
    # unittest.TextTestRunner().run(TestParquetDB('test_batch_reading'))
    # unittest.TextTestRunner().run(TestParquetDB('test_create_and_read'))
    # unittest.TextTestRunner().run(TestParquetDB('test_update_multi_keys'))
    # unittest.TextTestRunner().run(TestParquetDB('test_fixed_shape_tensor'))
    # unittest.TextTestRunner().run(TestParquetDB('test_rename_dataset'))
    # unittest.TextTestRunner().run(TestParquetDB('test_update_with_new_field_included'))
    # unittest.TextTestRunner().run(TestParquetDB("test_python_objects"))
    # unittest.TextTestRunner().run(TestParquetDB('test_update_multi_keys'))
    # unittest.TextTestRunner().run(TestParquetDB("test_transform"))
    # unittest.TextTestRunner().run(TestParquetDB("test_filter_delete"))
    # unittest.TextTestRunner().run(TestParquetDB("test_drop_duplicates"))test_update_multi_keys

    # unittest.TextTestRunner().run(TestParquetDB("test_update_multi_keys"))

    # for x in range(500):
    #     print(f"Iteration {x+1}")
    #     unittest.TextTestRunner().run(TestParquetDB("test_update_multi_keys"))
