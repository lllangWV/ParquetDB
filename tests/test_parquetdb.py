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

# logger=logging.getLogger('parquetdb')
# logger.setLevel(logging.DEBUG)
# ch = logging.StreamHandler()
# ch.setLevel(logging.DEBUG)
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# ch.setFormatter(formatter)
# logger.addHandler(ch)


class TestParquetDB(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory for the database
        self.temp_dir = tempfile.mkdtemp()
        self.db = ParquetDB(db_path=self.temp_dir)
        self.table_name = 'test_table'

    def tearDown(self):
        # Remove the temporary directory after the test
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

if __name__ == '__main__':
    unittest.main()
