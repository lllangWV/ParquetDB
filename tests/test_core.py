import logging
import unittest
import shutil
import os
import tempfile

import numpy as np
from parquetdb import ParquetDB
import pyarrow as pa
import pyarrow.compute as pc
import pandas as pd

logger=logging.getLogger('parquetdb')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

# TODO: Create tests for nested structure updates
# TODO: Create tests for 

class TestParquetDB(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory for the database
        self.temp_dir = tempfile.mkdtemp()
        self.db = ParquetDB(db_path=self.temp_dir)
        self.table_name = 'test_table'

        # Create some test data
        self.test_data = [
            {'name': 'Alice', 'age': 30},
            {'name': 'Bob', 'age': 25},
            {'name': 'Charlie', 'age': 35}
        ]
        self.test_df = pd.DataFrame(self.test_data)

    def tearDown(self):
        # Remove the temporary directory after the test
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_create_and_read(self):
        # Test creating data and reading it back
        data = [
            {'name': 'Alice', 'age': 30},
            {'name': 'Bob', 'age': 25}
        ]
        self.db.create(data, table_name=self.table_name)

        # Read the data back
        result = self.db.read(table_name=self.table_name)
        df = result.to_pandas()

        # Assertions
        self.assertEqual(len(df), 2)
        self.assertIn('name', df.columns)
        self.assertIn('age', df.columns)
        self.assertEqual(df.iloc[0]['name'], 'Alice')
        self.assertEqual(df.iloc[1]['name'], 'Bob')

    def test_update(self):
        # Test updating existing records

        data = [
            {'name': 'Charlie', 'age': 28},
            {'name': 'Diana', 'age': 32}
        ]
        self.db.create(data, table_name=self.table_name)

        # Update the age of 'Charlie'
        update_data = [
            {'id': 0, 'age': 29}
        ]
        self.db.update(update_data, table_name=self.table_name)

        # Read the data back
        result = self.db.read(table_name=self.table_name)
        df = result.to_pandas()

        # Assertions
        self.assertEqual(df.iloc[0]['age'], 29)
        self.assertEqual(df.iloc[1]['age'], 32)

    def test_delete(self):
        # Test deleting records
        data = [
            {'name': 'Eve', 'age': 35},
            {'name': 'Frank', 'age': 40}
        ]
        self.db.create(data, table_name=self.table_name)

        # Delete 'Eve'
        self.db.delete(ids=[0], table_name=self.table_name)

        # Read the data back
        result = self.db.read(table_name=self.table_name)
        df = result.to_pandas()

        # Assertions
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]['name'], 'Frank')

    def test_filters(self):
        # Test reading data with filters
        data = [
            {'name': 'Grace', 'age': 22},
            {'name': 'Heidi', 'age': 27},
            {'name': 'Ivan', 'age': 35}
        ]
        self.db.create(data, table_name=self.table_name)

        # Apply filter to get people older than 25
        age_filter = pc.field('age') > 25
        result = self.db.read(table_name=self.table_name, filters=[age_filter])
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
        self.db.create(data, table_name=self.table_name)

        # Add new data with an additional field
        new_data = [
            {'name': 'Karl', 'occupation': 'Engineer'}
        ]
        self.db.create(new_data, table_name=self.table_name)

        # Read back the data
        result = self.db.read(table_name=self.table_name)
        df = result.to_pandas()

        # Assertions
        self.assertIn('occupation', df.columns)
        self.assertEqual(df.iloc[1]['occupation'], 'Engineer')
        self.assertTrue(pd.isnull(df.iloc[0]['occupation']))
        self.assertTrue(np.isnan(df.iloc[1]['age']))

    def test_get_schema(self):
        # Test retrieving the schema
        data = [
            {'name': 'Liam', 'age': 45}
        ]
        self.db.create(data, table_name=self.table_name)
        schema = self.db.get_schema(table_name=self.table_name)

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
        self.db.create(data, table_name=self.table_name)

        # Read only the 'name' column
        result = self.db.read(table_name=self.table_name, columns=['name'])
        df = result.to_pandas()

        # Assertions
        self.assertEqual(len(df.columns), 1)
        self.assertIn('name', df.columns)
        self.assertNotIn('age', df.columns)
        self.assertNotIn('city', df.columns)

    def test_batch_reading(self):
        # Test reading data in batches
        data = [{'name': f'Person {i}', 'age': i} for i in range(100)]
        self.db.create(data, table_name=self.table_name)

        # Read data in batches of 20
        batches = self.db.read(table_name=self.table_name, batch_size=20, output_format='batch_generator')

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
        self.db.create(data, table_name=self.table_name)

        # Update the 'age' field to be a float instead of int
        new_field = pa.field('age', pa.float64())
        field_dict = {'age': new_field}
        self.db.update_schema(table_name=self.table_name, field_dict=field_dict)

        # Read back the data
        result = self.db.read(table_name=self.table_name)
        df = result.to_pandas()

        # Assertions
        self.assertEqual(df['age'].dtype, 'float64')

    def test_update_with_new_field_included(self):
        # Test updating the schema of the table
        data = [
            {'name': 'Mia', 'age': 30, 'city': 'New York'},
            {'name': 'Noah', 'age': 35, 'city': 'San Francisco'}
        ]
        self.db.create(data, table_name=self.table_name)

        # Update the 'Mia' record to include a new field and change age to 60
        data = {'id':0, 'age': 60, 'state':'NY'}
        self.db.update(data,table_name=self.table_name)

        # Read back the data
        result = self.db.read(table_name=self.table_name)
        df = result.to_pandas()

        # Assertions
        # print(df.head())
        self.assertEqual(df.iloc[0]['state'], 'NY')
        self.assertEqual(df.iloc[1]['state'], None)
        self.assertEqual(df.iloc[0]['age'], 60)
        self.assertEqual(df.iloc[1]['age'], 35)

    def test_invalid_table_name(self):
        # Test using a reserved table name
        with self.assertRaises(ValueError):
            self.db.create(data=[], table_name='tmp')

    def test_delete_nonexistent_id(self):
        # Test deleting an ID that doesn't exist
        data = [
            {'name': 'Peter', 'age': 50}
        ]
        self.db.create(data, table_name=self.table_name)

        # Attempt to delete a non-existent ID
        self.db.delete(ids=[999], table_name=self.table_name)

        # Read back the data
        result = self.db.read(table_name=self.table_name)
        df = result.to_pandas()

        # Assertions
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]['name'], 'Peter')

    def test_update_nonexistent_id(self):
        # Test updating an ID that doesn't exist
        data = [
            {'name': 'Quinn', 'age': 40}
        ]
        self.db.create(data, table_name=self.table_name)

        # Attempt to update a non-existent ID
        update_data = [
            {'id': 999, 'age': 41}
        ]
        with self.assertRaises(ValueError):
            self.db.update(update_data, table_name=self.table_name)

    def test_get_tables(self):
        # Should return a list containing 'test_table'
        self.db.create(data=self.test_data, table_name='test_table')

        tables = self.db.get_tables()
        self.assertIn('test_table', tables)
        self.assertIsInstance(tables, list)

    def test_get_metadata(self):
        self.db.create(data=self.test_data, 
                       table_name='test_table',
                       metadata={'key1':'value1', 'key2':'value2'})
        # Should return metadata dictionary (can be empty)
        metadata = self.db.get_metadata('test_table')
        self.assertIsInstance(metadata, dict)

        # Test for a non-existent table
        with self.assertRaises(ValueError):
            self.db.get_metadata('non_existent_table')

    def test_table_exists(self):
        self.db.create(data=self.test_data, table_name='test_table')
        # Should return True for existing table
        self.assertTrue(self.db.table_exists('test_table'))

        # Should return False for non-existent table
        self.assertFalse(self.db.table_exists('non_existent_table'))

    def test_drop_table(self):
        self.db.create(data=self.test_data, table_name='test_table')
        # Drop the table and check if it no longer exists
        self.db.drop_table('test_table')
        self.assertFalse(self.db.table_exists('test_table'))

    def test_rename_table(self):
        self.db.create(data=self.test_data, table_name='test_table')
        # Rename the table and check if the new name exists
        self.db.rename_table('test_table', 'renamed_table')
        self.assertTrue(self.db.table_exists('renamed_table'))
        self.assertFalse(self.db.table_exists('test_table'))

        # Attempt to rename to a reserved name
        with self.assertRaises(ValueError):
            self.db.rename_table('renamed_table', 'tmp')

        # Attempt to rename a non-existent table
        with self.assertRaises(ValueError):
            self.db.rename_table('non_existent_table', 'new_table')

    def test_copy_table(self):
        self.db.create(data=self.test_data, table_name='test_table')
        # Copy the table and check if both exist
        self.db.copy_table('test_table', 'copied_table')
        self.assertTrue(self.db.table_exists('test_table'))
        self.assertTrue(self.db.table_exists('copied_table'))

        # Verify data in copied table
        original_data = self.db.read(table_name='test_table').to_pandas()
        copied_data = self.db.read(table_name='copied_table').to_pandas()
        pd.testing.assert_frame_equal(original_data, copied_data)

        # Attempt to copy to an existing table without overwrite
        with self.assertRaises(ValueError):
            self.db.copy_table('test_table', 'copied_table')

        # Copy with overwrite
        self.db.copy_table('test_table', 'copied_table', overwrite=True)
        self.assertTrue(self.db.table_exists('copied_table'))

    def test_export_table(self):
        self.db.create(data=self.test_data, table_name='test_table')
        # Export the table to CSV
        export_path = os.path.join(self.temp_dir, 'exported_table.csv')
        self.db.export_table('test_table', export_path, format='csv')
        self.assertTrue(os.path.exists(export_path))

        # Verify the exported data
        exported_df = pd.read_csv(export_path)
        original_df = self.db.read(table_name='test_table').to_pandas()
        pd.testing.assert_frame_equal(original_df, exported_df)

        # Export to an unsupported format
        with self.assertRaises(ValueError):
            self.db.export_table('test_table', export_path, format='xlsx')

    def test_merge_tables(self):
        self.db.create(data=self.test_data, table_name='test_table')
        # Create another table
        additional_data = [
            {'id': 4, 'name': 'Dave', 'age': 40},
            {'id': 5, 'name': 'Eve', 'age': 45}
        ]
        self.db.create(data=additional_data, table_name='additional_table')

        # Attempt to merge tables (method not implemented)
        with self.assertRaises(NotImplementedError):
            self.db.merge_tables(['test_table', 'additional_table'], 'merged_table')

    # def test_deep_update(self):
    #     original_value = {'a': 1, 'b': {'c': 2, 'd': 3}}
    #     update_value = {'a': 10, 'b': {'c': 20, 'e': 30}}
    #     expected_value = {'a': 10, 'b': {'c': 20, 'd': 3, 'e': 30}}
    #     result = deep_update(original_value, update_value)
    #     self.assertEqual(result, expected_value)

if __name__ == '__main__':
    unittest.main()
