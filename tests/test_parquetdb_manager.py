import logging
import unittest
import shutil
import os
import tempfile

import numpy as np
from parquetdb import ParquetDBManager, config
import pyarrow as pa
import pyarrow.compute as pc
import pandas as pd

logger=logging.getLogger('tests')

config.logging_config.loggers.timing.level='ERROR'
config.logging_config.loggers.parquetdb.level='ERROR'
config.logging_config.loggers.tests.level='DEBUG'
config.apply()

class TestParquetDBManager(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory for the database
        self.temp_dir = tempfile.mkdtemp()
        self.db = ParquetDBManager(datasets_dir=self.temp_dir)
        self.dataset_name = 'test_dataset'

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
        self.db.create(data, dataset_name=self.dataset_name)

        
        data = [
            {'name': 'Alice', 'age': 30},
            {'name': 'Bob', 'age': 25}
        ]
        self.db.create(data,dataset_name=self.dataset_name)
        
        # Read the data back
        result = self.db.read(dataset_name=self.dataset_name)
        df = result.to_pandas()

        # Assertions
        self.assertEqual(len(df), 4)
        self.assertIn('name', df.columns)
        self.assertIn('age', df.columns)
        self.assertEqual(df[df['age'] == 30].iloc[0]['name'], 'Alice')
        self.assertEqual(df[df['age'] == 25].iloc[0]['name'], 'Bob')

    def test_update(self):
        # Test updating existing records

        data = [
            {'name': 'Charlie', 'age': 28},
            {'name': 'Diana', 'age': 32}
        ]
        self.db.create(data, dataset_name=self.dataset_name)

        # Update the age of 'Charlie'
        update_data = [
            {'id': 0, 'age': 29}
        ]
        self.db.update(update_data, dataset_name=self.dataset_name)

        # Read the data back
        result = self.db.read(dataset_name=self.dataset_name)
        df = result.to_pandas()

        # Assertions
        self.assertEqual(df[df['name'] == 'Charlie'].iloc[0]['age'], 29)
        self.assertEqual(df[df['name'] == 'Diana'].iloc[0]['age'], 32)

    def test_delete(self):
        # Test deleting records
        data = [
            {'name': 'Eve', 'age': 35},
            {'name': 'Frank', 'age': 40}
        ]
        self.db.create(data, dataset_name=self.dataset_name)

        # Delete 'Eve'
        self.db.delete(ids=[0], dataset_name=self.dataset_name)

        # Read the data back
        result = self.db.read(dataset_name=self.dataset_name)
        df = result.to_pandas()

        # Assertions
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]['name'], 'Frank')
        
    def test_create_and_normalize(self):
        # Step 1: Create data without normalization
        self.db.create(data=self.test_df, dataset_name=self.dataset_name, normalize_dataset=False)

        # Step 2: Verify that data has been written to the dataset directory
        dataset_files = self.db.get_current_files(dataset_name=self.dataset_name)
        self.assertGreater(len(dataset_files), 0, "No parquet files found after create without normalization.")

        # Load the data to check its state before normalization
        loaded_data = self.db.read(dataset_name=self.dataset_name)
        self.assertEqual(loaded_data.num_rows, len(self.test_data), "Mismatch in row count before normalization.")

        # Step 3: Run normalization. Will normalize to 1 row per file and 1 row per group
        self.db.normalize(dataset_name=self.dataset_name, max_rows_per_file= 1, max_rows_per_group = 1)

        # Step 4: Verify that the data has been normalized (e.g., consistent row distribution)
        normalized_data = self.db.read(dataset_name=self.dataset_name)
        self.assertEqual(normalized_data.num_rows, 3, "Mismatch in row count after normalization.")

        # Additional checks to ensure normalization affects file structure (if applicable)
        normalized_files = self.db.get_current_files(dataset_name=self.dataset_name)
        self.assertGreaterEqual(len(normalized_files), 3, "No files found after normalization.")

    def test_filters(self):
        # Test reading data with filters
        data = [
            {'name': 'Grace', 'age': 22},
            {'name': 'Heidi', 'age': 27},
            {'name': 'Ivan', 'age': 35}
        ]
        self.db.create(data, dataset_name=self.dataset_name)

        # Apply filter to get people older than 25
        age_filter = pc.field('age') > 25
        result = self.db.read(dataset_name=self.dataset_name, filters=[age_filter])
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
        self.db.create(data, dataset_name=self.dataset_name)

        # Add new data with an additional field
        new_data = [
            {'name': 'Karl', 'occupation': 'Engineer'}
        ]
        self.db.create(new_data, dataset_name=self.dataset_name)

        # Read back the data
        result = self.db.read(dataset_name=self.dataset_name)
        df = result.to_pandas()

        # Assertions
        self.assertIn('occupation', df.columns)
        self.assertEqual(df[df['name'] == 'Karl'].iloc[0]['occupation'], 'Engineer')
        self.assertTrue(pd.isnull(df[df['name'] == 'Judy'].iloc[0]['occupation']))
        self.assertTrue(np.isnan(df[df['name'] == 'Karl'].iloc[0]['age']))

    def test_get_schema(self):
        # Test retrieving the schema
        data = [
            {'name': 'Liam', 'age': 45}
        ]
        self.db.create(data, dataset_name=self.dataset_name)
        schema = self.db.get_schema(dataset_name=self.dataset_name)

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
        self.db.create(data, dataset_name=self.dataset_name)

        # Read only the 'name' column
        result = self.db.read(dataset_name=self.dataset_name, columns=['name'])
        df = result.to_pandas()

        # Assertions
        self.assertEqual(len(df.columns), 1)
        self.assertIn('name', df.columns)
        self.assertNotIn('age', df.columns)
        self.assertNotIn('city', df.columns)

    def test_batch_reading(self):
        # Test reading data in batches
        data = [{'name': f'Person {i}', 'age': i} for i in range(100)]
        self.db.create(data, dataset_name=self.dataset_name)

        # Read data in batches of 20
        batches = self.db.read(dataset_name=self.dataset_name, batch_size=20, output_format='batch_generator')

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
        # Test updating the schema of the dataset
        data = [
            {'name': 'Olivia', 'age': 29}
        ]
        self.db.create(data, dataset_name=self.dataset_name)

        # Update the 'age' field to be a float instead of int
        new_field = pa.field('age', pa.float64())
        field_dict = {'age': new_field}
        self.db.update_schema(dataset_name=self.dataset_name, field_dict=field_dict)

        # Read back the data
        result = self.db.read(dataset_name=self.dataset_name)
        df = result.to_pandas()

        # Assertions
        self.assertEqual(df['age'].dtype, 'float64')

    def test_update_with_new_field_included(self):
        # Test updating the schema of the dataset
        data = [
            {'name': 'Mia', 'age': 30, 'city': 'New York'},
            {'name': 'Noah', 'age': 35, 'city': 'San Francisco'}
        ]
        self.db.create(data, dataset_name=self.dataset_name)

        # Update the 'Mia' record to include a new field and change age to 60
        data = {'id':0, 'age': 60, 'state':'NY'}
        self.db.update(data,dataset_name=self.dataset_name)

        # Read back the data
        result = self.db.read(dataset_name=self.dataset_name)
        df = result.to_pandas()

        # Assertions
        # print(df.head())
        self.assertEqual(df.iloc[0]['state'], 'NY')
        self.assertEqual(df.iloc[1]['state'], None)
        self.assertEqual(df.iloc[0]['age'], 60)
        self.assertEqual(df.iloc[1]['age'], 35)

    def test_invalid_dataset_name(self):
        # Test using a reserved dataset name
        with self.assertRaises(ValueError):
            self.db.create(data=[], dataset_name='tmp')

    def test_delete_nonexistent_id(self):
        # Test deleting an ID that doesn't exist
        data = [
            {'name': 'Peter', 'age': 50}
        ]
        self.db.create(data, dataset_name=self.dataset_name)

        # Attempt to delete a non-existent ID
        self.db.delete(ids=[999], dataset_name=self.dataset_name)

        # Read back the data
        result = self.db.read(dataset_name=self.dataset_name)
        df = result.to_pandas()

        # Assertions
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]['name'], 'Peter')

    def test_get_datasets(self):
        # Should return a list containing 'test_dataset'
        self.db.create(data=self.test_data, dataset_name='test_dataset')

        datasets = self.db.get_datasets()
        self.assertIn('test_dataset', datasets)
        self.assertIsInstance(datasets, list)

    def test_get_metadata(self):
        self.db.create(data=self.test_data, 
                       dataset_name='test_dataset',
                       metadata={'key1':'value1', 'key2':'value2'})
        # Should return metadata dictionary (can be empty)
        metadata = self.db.get_metadata(dataset_name='test_dataset')
        self.assertIsInstance(metadata, dict)

        # Test for a non-existent dataset
        with self.assertRaises(ValueError):
            self.db.get_metadata(dataset_name='non_existent_dataset')

    def test_dataset_exists(self):
        self.db.create(data=self.test_data, dataset_name='test_dataset')
        # Should return True for existing dataset
        self.assertTrue(self.db.dataset_exists('test_dataset'))

        # Should return False for non-existent dataset
        self.assertFalse(self.db.dataset_exists('non_existent_dataset'))

    def test_drop_dataset(self):
        self.db.create(data=self.test_data, dataset_name='test_dataset')
        # Drop the dataset and check if it no longer exists
        self.db.drop_dataset('test_dataset')
        self.assertFalse(self.db.dataset_exists('test_dataset'))

    def test_rename_dataset(self):
        self.db.create(data=self.test_data, dataset_name='test_dataset')
        # Rename the dataset and check if the new name exists
        self.db.rename_dataset(dataset_name='test_dataset', new_name='renamed_dataset')
        self.assertTrue(self.db.dataset_exists('renamed_dataset'))
        self.assertFalse(self.db.dataset_exists('test_dataset'))

        # Attempt to rename to a reserved name
        with self.assertRaises(ValueError):
            self.db.rename_dataset(dataset_name='renamed_dataset', new_name='tmp')

        # Attempt to rename a non-existent dataset
        with self.assertRaises(ValueError):
            self.db.rename_dataset(dataset_name='non_existent_dataset', new_name='new_dataset')

    def test_copy_dataset(self):
        self.db.create(data=self.test_data, dataset_name='test_dataset')
        # Copy the dataset and check if both exist
        self.db.copy_dataset(dataset_name='test_dataset', dest_name='copied_dataset')
        self.assertTrue(self.db.dataset_exists('test_dataset'))
        self.assertTrue(self.db.dataset_exists('copied_dataset'))

        # Verify data in copied dataset
        original_data = self.db.read(dataset_name='test_dataset').to_pandas()
        copied_data = self.db.read(dataset_name='copied_dataset').to_pandas()
        pd.testing.assert_frame_equal(original_data, copied_data)

        # Attempt to copy to an existing dataset without overwrite
        with self.assertRaises(ValueError):
            self.db.copy_dataset(dataset_name='test_dataset', dest_name='copied_dataset')

        # Copy with overwrite
        self.db.copy_dataset(dataset_name='test_dataset', dest_name='copied_dataset', overwrite=True)
        self.assertTrue(self.db.dataset_exists('copied_dataset'))

    def test_export_dataset(self):
        self.db.create(data=self.test_data, dataset_name='test_dataset')
        # Export the dataset to CSV
        export_path = os.path.join(self.temp_dir, 'exported_dataset.csv')
        self.db.export_dataset('test_dataset', export_path, format='csv')
        self.assertTrue(os.path.exists(export_path))

        # Verify the exported data
        exported_df = pd.read_csv(export_path)
        original_df = self.db.read(dataset_name='test_dataset').to_pandas()
        pd.testing.assert_frame_equal(original_df, exported_df)

        # Export to an unsupported format
        with self.assertRaises(ValueError):
            self.db.export_dataset('test_dataset', export_path, format='xlsx')

    def test_merge_datasets(self):
        self.db.create(data=self.test_data, dataset_name='test_dataset')
        # Create another dataset
        additional_data = [
            {'name': 'Dave', 'age': 40},
            {'name': 'Eve', 'age': 45}
        ]
        self.db.create(data=additional_data, dataset_name='additional_dataset')

        # Attempt to merge datasets (method not implemented)
        with self.assertRaises(NotImplementedError):
            self.db.merge_datasets(['test_dataset', 'additional_dataset'], 'merged_dataset')

    # def test_deep_update(self):
    #     original_value = {'a': 1, 'b': {'c': 2, 'd': 3}}
    #     update_value = {'a': 10, 'b': {'c': 20, 'e': 30}}
    #     expected_value = {'a': 10, 'b': {'c': 20, 'd': 3, 'e': 30}}
    #     result = deep_update(original_value, update_value)
    #     self.assertEqual(result, expected_value)

if __name__ == '__main__':
    unittest.main()


# if __name__ == "__main__":
#     unittest.TextTestRunner().run(TestParquetDBManager('test_add_new_field'))