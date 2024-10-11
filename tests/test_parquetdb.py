import logging
import time
import unittest
import shutil
import os
import tempfile

import numpy as np
from parquetdb import ParquetDB, config
import pyarrow as pa
import pyarrow.compute as pc
import pandas as pd

logger=logging.getLogger('tests')

config.logging_config.loggers.timing.level='ERROR'
config.logging_config.loggers.parquetdb.level='ERROR'
config.logging_config.loggers.tests.level='DEBUG'
config.apply()


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
        # time.sleep(0.01)

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
        self.db.create(data, )

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
        self.db.normalize( max_rows_per_file= 1, max_rows_per_group = 1)

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


    # def test_nested_empty_struct_field(self):
    #     # Test adding data with a new field and ensure schema evolves
    #     # This also test that if new incoming data doesn't have a 
    #     # field that is already in the schema, a null value of the correct type is added
    #     data = [
    #         {'name': {}, 'age': {"field1": {"inner_field1": 1, "inner_field2": 2}}},
    #     ]
    #     self.db.create(data)

    #     # Add new data with an additional field
    #     new_data = [
    #         {'name': {'age':1}, 'occupation': 'Engineer'}
    #     ]
    #     self.db.create(new_data)

    #     # Read back the data
    #     result = self.db.read()
    #     df = result.to_pandas()
        
    #     # Assertions
    #     # self.assertIn('occupation', df.columns)
    #     print(df.iloc[0]['name'])
    #     self.assertIn('dummy_field', df.iloc[0]['name'])
    #     self.assertEqual(df.iloc[0]['name']['dummy_field'], None)
    #     self.assertEqual(df.iloc[1]['name']['dummy_field'], None)
    #     self.assertEqual(df.iloc[0]['age']['field1']['inner_field1'], 1)
    #     self.assertEqual(df.iloc[1]['name']['age'], 1)

    def test_get_schema(self):
        # Test retrieving the schema
        data = [
            {'name': 'Liam', 'age': 45}
        ]
        self.db.create(data, )
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
        batches = self.db.read(batch_size=20, output_format='batch_generator')

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
        self.db.update(data,)

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

        # Attempt to rename to a reserved name
        with self.assertRaises(ValueError):
            self.db.rename_dataset('tmp')

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

    # def test_merge_datasets(self):
    #     self.db.create(data=self.test_data)
    #     # Create another table
    #     additional_data = [
    #         {'id': 4, 'name': 'Dave', 'age': 40},
    #         {'id': 5, 'name': 'Eve', 'age': 45}
    #     ]
    #     self.db.create(data=additional_data, dataset_name='additional_table')

    #     # Attempt to merge tables (method not implemented)
    #     with self.assertRaises(NotImplementedError):
    #         self.db.merge_datasets(['test_table', 'additional_table'], 'merged_table')

    # def test_deep_update(self):
    #     original_value = {'a': 1, 'b': {'c': 2, 'd': 3}}
    #     update_value = {'a': 10, 'b': {'c': 20, 'e': 30}}
    #     expected_value = {'a': 10, 'b': {'c': 20, 'd': 3, 'e': 30}}
    #     result = deep_update(original_value, update_value)
    #     self.assertEqual(result, expected_value)

if __name__ == '__main__':
    unittest.main()
    
# if __name__ == '__main__':
#     for x in range(20):
#         print(f"Iteration {x+1}")
        
#         # # Create a test suite and add your test case
#         # suite = unittest.TestLoader().run(TestParquetDB('test_add_new_field'))
#         # Create a test suite and add your test case
#         suite = unittest.TestLoader().loadTestsFromTestCase(TestParquetDB)
        
#         # Run the tests
#         unittest.TextTestRunner().run(suite)
#         # unittest.TextTestRunner().run(TestParquetDB('test_add_new_field'))


# if __name__ == "__main__":
#     unittest.TextTestRunner().run(TestParquetDB('test_update_schema'))

# if __name__ == "__main__":
#     unittest.TextTestRunner().run(TestParquetDB('test_add_new_field'))