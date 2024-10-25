import json
import logging
import time
import unittest
import shutil
import os
import tempfile

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pandas as pd

from parquetdb import ParquetDB, config
from parquetdb.core.parquetdb import LoadConfig, NormalizeConfig

logger=logging.getLogger('tests')

config.logging_config.loggers.timing.level='ERROR'
config.logging_config.loggers.parquetdb.level='ERROR'
config.logging_config.loggers.tests.level='DEBUG'
config.apply()

with open(os.path.join(config.tests_dir,'data', 'alexandria_test.json'), 'r') as f:
    alexandria_data = json.load(f)

class TestParquetDB(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory for the database
        self.temp_dir = tempfile.mkdtemp()
        self.dataset_name='test_dataset'
        self.db = ParquetDB(dataset_name=self.dataset_name, dir=self.temp_dir)

        # Create some test data
        self.test_data = [
            {'name': 'Alice', 'age': 30},
            {'name': 'Bob', 'age': 25},
            {'name': 'Charlie', 'age': 35}
        ]
        logger.debug(f"Test data: {self.test_data}")
        self.test_df = pd.DataFrame(self.test_data)

    def tearDown(self):
        # Remove the temporary directory after the test
        try:
            shutil.rmtree(self.temp_dir)
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
        # For some reason, there are race conditions when 
        # deleting the directory and performaing another test
        # time.sleep(0.1)

    def test_create_and_read(self):
        logger.info("Testing create and read")
        # Test creating data and reading it back
        data = [
            {'name': 'Alice', 'age': 30},
            {'name': 'Bob', 'age': 25}
        ]
        self.db.create(data)
        
        table=self.db.read()
        df=table.to_pandas()
        logger.debug(f"DataFrame:\n{df}")
        data = [
            {'name': 'Alice', 'age': 30},
            {'name': 'Bob', 'age': 25}
        ]
        self.db.create(data)

        # Read the data back
        table=self.db.read()
        df=table.to_pandas()
        logger.debug(f"DataFrame:\n{df}")
        
        # Assertions
        self.assertEqual(len(df), 4)
        self.assertIn('name', df.columns)
        self.assertIn('age', df.columns)
        self.assertEqual(df[df['age'] == 30].iloc[0]['name'], 'Alice')
        self.assertEqual(df[df['age'] == 25].iloc[0]['name'], 'Bob')
        
        logger.info("Test create and read passed")

    def test_update(self):
        logger.info("Testing update")
        # Test updating existing records

        data = [
            {'name': 'Charlie', 'age': 28},
            {'name': 'Diana', 'age': 32}
        ]
        self.db.create(data)
        
        # Read the data back
        result = self.db.read()
        df = result.to_pandas()
        logger.debug(f"DataFrame:\n{df}")

        # Update the age of 'Charlie'
        update_data = [
            {'id': 0, 'age': 29}
        ]
        self.db.update(update_data)

        # Read the data back
        result = self.db.read()
        df = result.to_pandas()
        logger.debug(f"DataFrame:\n{df}")
        
        
        # Assertions
        self.assertEqual(df[df['name'] == 'Charlie'].iloc[0]['age'], 29)
        self.assertEqual(df[df['name'] == 'Diana'].iloc[0]['age'], 32)
        logger.info("Test update passed")

    def test_delete(self):
        # Test deleting records
        data = [
            {'name': 'Eve', 'age': 35},
            {'name': 'Frank', 'age': 40}
        ]
        self.db.create(data)

        # Delete 'Eve'
        self.db.delete(ids=[0], )

        # Read the data back
        result = self.db.read()
        df = result.to_pandas()

        # Assertions
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]['name'], 'Frank')
        
    def test_create_and_normalize(self):
        # Step 1: Create data without normalization
        self.db.create(data=self.test_df, normalize_dataset=False)

        # Step 2: Verify that data has been written to the dataset directory
        dataset_files = self.db.get_current_files()
        self.assertGreater(len(dataset_files), 0, "No parquet files found after create without normalization.")

        # Load the data to check its state before normalization
        loaded_data = self.db.read()
        self.assertEqual(loaded_data.num_rows, len(self.test_data), "Mismatch in row count before normalization.")

        # Step 3: Run normalization. Will normalize to 1 row per file and 1 row per group
        self.db.normalize( normalize_config=NormalizeConfig(max_rows_per_file= 1, min_rows_per_group = 1, max_rows_per_group = 1))

        # Step 4: Verify that the data has been normalized (e.g., consistent row distribution)
        normalized_data = self.db.read()
        self.assertEqual(normalized_data.num_rows, 3, "Mismatch in row count after normalization.")

        # Additional checks to ensure normalization affects file structure (if applicable)
        normalized_files = self.db.get_current_files()
        self.assertGreaterEqual(len(normalized_files), 3, "No files found after normalization.")

    def test_filters(self):
        # Test reading data with filters
        data = [
            {'name': 'Grace', 'age': 22},
            {'name': 'Heidi', 'age': 27},
            {'name': 'Ivan', 'age': 35}
        ]
        self.db.create(data)

        # Apply filter to get people older than 25
        age_filter = pc.field('age') > 25
        result = self.db.read(filters=[age_filter])
        df = result.to_pandas()

        # Assertions
        self.assertEqual(len(df), 2)
        self.assertListEqual(df['name'].tolist(), ['Heidi', 'Ivan'])

    def test_add_new_field(self):
        # Test adding data with a new field and ensure schema evolves
        # This also test that if new incoming data doesn't have a 
        # field that is already in the schema, a null value of the correct type is added
        data = [
            {'name': 'Judy', 'age': 29}
        ]
        self.db.create(data)

        # Add new data with an additional field
        new_data = [
            {'name': 'Karl', 'occupation': 'Engineer'}
        ]
        self.db.create(new_data)

        # Read back the data
        table = self.db.read()
        df = table.to_pandas()
        
        # Assertions
        self.assertIn('occupation', df.columns)
        self.assertEqual(df[df['name'] == 'Karl'].iloc[0]['occupation'], 'Engineer')
        self.assertTrue(pd.isnull(df[df['name'] == 'Judy'].iloc[0]['occupation']))
        self.assertTrue(np.isnan(df[df['name'] == 'Karl'].iloc[0]['age']))
        
    def test_nested_data_handling(self):
    
        py_lattice_1 = [[[1.1, 0, 0], [0, 1, 0], [0, 0, 1]], [[1, 0, 0], [0, 1, 0], [0, 0, 1]]]
        py_lattice_2 = [[[2.1, 0, 0], [0, 2, 0], [0, 0, 2]], [[2, 0, 0], [0, 2, 0], [0, 0, 2]]]

        current_data = [
            {'b': {'x': 10, 'y': 20}, 'c': {}, 'e': 5, 'lattice': py_lattice_1, 'unordered_list_string': ['hi', 'reree'], 'unordered_list_int': [0, 1, 3]},
            {'b': {'x': 30, 'y': 40}, 'c': {}, 'lattice': py_lattice_2, 'unordered_list_string': ['reree'], 'unordered_list_int': [0, 1]},
            {'b': {'x': 30, 'y': 40}, 'c': {}},
            {'b': {'x': 30, 'y': 40}, 'c': {}},
            {'b': {'x': 30, 'y': 40}, 'c': {}},
            {'b': {'x': 30, 'y': 40}, 'c': {}, 'lattice': py_lattice_2, 'unordered_list_string': ['reree'], 'unordered_list_int': [0, 1]},
        ]

        incoming_data = [
            {'id': 2, 'b': {'x': 10, 'y': 20}, 'c': {}},
            {'id': 0, 'b': {'y': 50, 'z': {'a': 1.1, 'b': 3.3}}, 'c': {}, 'd': 1, 'e': 6, 'lattice': py_lattice_2, 'unordered_list_string': ['updated']},
            {'id': 3, 'b': {'y': 50, 'z': {'a': 4.5, 'b': 5.3}}, 'c': {}, 'd': 1, 'e': 6, 'lattice': py_lattice_2, 'unordered_list_string': ['updated']},
        ]

        self.db.create(current_data)
        
        # Initial read before the update
        table = self.db.read()
        df = table.to_pandas()

        # Assert the shape before the update (confirming initial data structure)
        assert df.shape[0] == len(current_data), f"Expected {len(current_data)} rows before update, but got {df.shape[0]}"
        assert df.loc[0, 'b.x'] == 10
        assert df.loc[0, 'b.y'] == 20
        assert np.isnan(df.loc[0, 'c.dummy_field'])
        assert df.loc[0, 'e'] == 5
        assert np.array_equal(table['lattice'].combine_chunks().to_numpy_ndarray()[0], np.array(py_lattice_1)), "Row 0 lattice data mismatch after update"
        assert df.loc[0, 'unordered_list_string'][0] == 'hi', "Row 0 'unordered_list_string' did not update correctly"
        assert df.loc[0, 'unordered_list_string'][1] == 'reree', "Row 0 'unordered_list_string' did not update correctly"
        assert df.loc[0, 'unordered_list_int'][0] == 0, "Row 0 'unordered_list_int' did not update correctly"
        assert df.loc[0, 'unordered_list_int'][1] == 1, "Row 0 'unordered_list_int' did not update correctly"
        assert df.loc[0, 'unordered_list_int'][2] == 3, "Row 0 'unordered_list_int' did not update correctly"
        
        assert df.loc[1, 'b.x'] == 30
        assert df.loc[1, 'b.y'] == 40
        assert np.isnan(df.loc[1, 'c.dummy_field'])
        assert np.isnan(df.loc[1, 'e'])
        assert np.array_equal(table['lattice'].combine_chunks().to_numpy_ndarray()[1], np.array(py_lattice_2)), "Row 0 lattice data mismatch after update"
        assert df.loc[1, 'unordered_list_string'][0] == 'reree', "Row 0 'unordered_list_string' did not update correctly"
        assert df.loc[1, 'unordered_list_int'][0] == 0, "Row 0 'unordered_list_int' did not update correctly"
        assert df.loc[1, 'unordered_list_int'][1] == 1, "Row 0 'unordered_list_int' did not update correctly"
        
        
        assert df.loc[2, 'b.x'] == 30
        assert df.loc[2, 'b.y'] == 40
        assert np.isnan(df.loc[2, 'c.dummy_field'])
        assert np.isnan(df.loc[2, 'e'])
        assert np.isnan(df.loc[2, 'e'])
        assert np.isnan(df.loc[2, 'e'])
        assert np.isnan(df.loc[2, 'lattice']).all()
        assert df.loc[2, 'unordered_list_string'] == None
        assert df.loc[2, 'unordered_list_int'] == None
        
        
        assert df.shape[0] == len(current_data), f"Expected {len(current_data)} rows before update, but got {df.shape[0]}"
        assert df.loc[5, 'b.x'] == 30
        assert df.loc[5, 'b.y'] == 40
        assert np.isnan(df.loc[5, 'c.dummy_field'])
        assert np.isnan(df.loc[5, 'e'])
        assert np.array_equal(table['lattice'].combine_chunks().to_numpy_ndarray()[5], np.array(py_lattice_2)), "Row 0 lattice data mismatch after update"
        assert df.loc[5, 'unordered_list_string'][0] == 'reree', "Row 0 'unordered_list_string' did not update correctly"
        assert df.loc[5, 'unordered_list_int'][0] == 0, "Row 0 'unordered_list_int' did not update correctly"
        assert df.loc[5, 'unordered_list_int'][1] == 1, "Row 0 'unordered_list_int' did not update correctly"
        
        
        # Perform the update
        self.db.update(incoming_data)
        
        # Read back the data after the update
        table = self.db.read()
        df = table.to_pandas()

        # Assert that the number of rows remains the same (no new rows added)
        assert df.shape[0] == len(current_data), f"Expected {len(current_data)} rows after update, but got {df.shape[0]}"
        
        # Check if the updates for `id:0` have been applied correctly
        assert df.loc[0, 'b.y'] == 50
        assert df.loc[0, 'b.x'] == 10
        assert df.loc[0, 'b.z.a'] == 1.1
        assert df.loc[0, 'b.z.b'] == 3.3
        assert df.loc[0, 'e'] == 6, "Row 0 column 'e' values mismatch after update"
        assert  df.loc[0, 'unordered_list_string'] == ['updated'], "Row 0 'unordered_list_string' did not update correctly"
        
        assert np.array_equal(table['lattice'].combine_chunks().to_numpy_ndarray()[0], np.array(py_lattice_2)), "Row 0 lattice data mismatch after update"
        
        # Check if the updates for `id:3` have been applied correctly
        assert df.loc[3, 'b.y'] == 50
        assert df.loc[3, 'b.x'] == 30
        assert df.loc[3, 'b.z.a'] == 4.5
        assert df.loc[3, 'b.z.b'] == 5.3
        assert df.loc[3, 'e'] == 6, "Row 3 column 'e' values mismatch after update"
        assert df.loc[3, 'unordered_list_string'] == ['updated'], "Row 3 'unordered_list_string' did not update correctly"
        assert np.array_equal(table['lattice'].combine_chunks().to_numpy_ndarray()[3], np.array(py_lattice_2)), "Row 3 lattice data mismatch after update"
        

        # Check that rows without 'id' in incoming data remain unchanged (e.g., row 1)
        assert df.loc[1, 'b.y'] == 40
        assert df.loc[1, 'b.x'] == 30
        assert df.loc[1, 'unordered_list_string'] == ['reree'], "Row 1 'unordered_list_string' unexpectedly changed"
        assert np.array_equal(table['lattice'].combine_chunks().to_numpy_ndarray()[1], np.array(py_lattice_2)), "Row 1 lattice data unexpectedly changed"
            
    def test_get_schema(self):
        # Test retrieving the schema
        data = [
            {'name': 'Liam', 'age': 45}
        ]
        self.db.create(data)
        schema = self.db.get_schema()

        # Assertions
        self.assertIn('name', schema.names)
        self.assertIn('age', schema.names)
        self.assertIn('id', schema.names)

    def test_read_specific_columns(self):
        # Test reading specific columns
        data = [
            {'name': 'Mia', 'age': 30, 'city': 'New York'},
            {'name': 'Noah', 'age': 35, 'city': 'San Francisco'}
        ]
        self.db.create(data)

        # Read only the 'name' column
        result = self.db.read(columns=['name'])
        df = result.to_pandas()

        # Assertions
        self.assertEqual(len(df.columns), 1)
        self.assertIn('name', df.columns)
        self.assertNotIn('age', df.columns)
        self.assertNotIn('city', df.columns)

    def test_batch_reading(self):
        # Test reading data in batches
        data = [{'name': f'Person {i}', 'age': i} for i in range(100)]
        self.db.create(data)

        # Read data in batches of 20
        batches = self.db.read(batch_size=20, load_format='batches')

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
        data = [
            {'name': 'Olivia', 'age': 29}
        ]
        self.db.create(data)
        
        # Read back the data
        result = self.db.read()
        df = result.to_pandas()
        
        logger.debug(f"DataFrame:\n{df}")
        
        # Update the 'age' field to be a float instead of int
        new_field = pa.field('age', pa.float64())
        field_dict = {'age': new_field}
        self.db.update_schema(field_dict=field_dict)

        # Read back the data
        result = self.db.read()
        df = result.to_pandas()
        
        logger.debug(f"DataFrame:\n{df}")

        # Assertions
        self.assertEqual(df['age'].dtype, 'float64')

    def test_update_with_new_field_included(self):
        # Test updating the schema of the table
        data = [
            {'name': 'Mia', 'age': 30, 'city': 'New York'},
            {'name': 'Noah', 'age': 35, 'city': 'San Francisco'}
        ]
        self.db.create(data)

        # Update the 'Mia' record to include a new field and change age to 60
        data = {'id':0, 'age': 60, 'state':'NY'}
        self.db.update(data)

        # Read back the data
        result = self.db.read()
        df = result.to_pandas()

        # Assertions
        self.assertEqual(df.iloc[0]['state'], 'NY')
        self.assertEqual(df.iloc[1]['state'], None)
        self.assertEqual(df.iloc[0]['age'], 60)
        self.assertEqual(df.iloc[1]['age'], 35)

    def test_delete_nonexistent_id(self):
        # Test deleting an ID that doesn't exist
        data = [
            {'name': 'Peter', 'age': 50}
        ]
        self.db.create(data)

        # Attempt to delete a non-existent ID
        self.db.delete(ids=[999])

        # Read back the data
        result = self.db.read()
        df = result.to_pandas()

        # Assertions
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]['name'], 'Peter')

    def test_get_metadata(self):
        self.db.create(data=self.test_data,
                       metadata={'key1':'value1', 'key2':'value2'})
        # Should return metadata dictionary (can be empty)
        metadata = self.db.get_metadata()
        self.assertIsInstance(metadata, dict)

    def test_drop_dataset(self):
        self.db.create(data=self.test_data)
        # Drop the table and check if it no longer exists
        self.db.drop_dataset()

    def test_rename_dataset(self):
        self.db.create(data=self.test_data)
        # Rename the table and check if the new name exists
        self.db.rename_dataset('renamed_table')


    def test_export_dataset(self):
        self.db.create(data=self.test_data)
        # Export the table to CSV
        export_path = os.path.join(self.temp_dir, 'exported_table.csv')
        self.db.export_dataset(export_path, format='csv')
        self.assertTrue(os.path.exists(export_path))

        # Verify the exported data
        exported_df = pd.read_csv(export_path)
        original_df = self.db.read().to_pandas()
        pd.testing.assert_frame_equal(original_df, exported_df)

        # Export to an unsupported format
        with self.assertRaises(ValueError):
            self.db.export_dataset(export_path, format='xlsx')

    # def test_alexandria_import(self):
    #     self.db.create(alexandria_data['entries'])
        
        
    #     # Initial read before the update
    #     table = self.db.read()
    #     df = table.to_pandas()

    #     print(df.loc[0, 'energy'])
    #     # Assert the shape before the update (confirming initial data structure)
    #     assert df.loc[0, 'energy'] == -80.83444733
    #     assert df.loc[0, 'correction'] == -80.83444733
    #     assert df.loc[0, 'parameters.correction'] == -80.83444733


if __name__ == '__main__':
    # unittest.TextTestRunner().run(TestParquetDB('test_alexandria_import'))
    unittest.main()
    
    
# if __name__ == '__main__':
#     for x in range(50):
#         print(f"Iteration {x+1}")
        
        # # Create a test suite and add your test case
        # suite = unittest.TestLoader().run(TestParquetDB('test_nested_data_handling'))
        # Create a test suite and add your test case
        # suite = unittest.TestLoader().loadTestsFromTestCase(TestParquetDB)
        
        # Run the tests
        # unittest.TextTestRunner().run(suite)
        # unittest.TextTestRunner().run(TestParquetDB('test_add_new_field'))


