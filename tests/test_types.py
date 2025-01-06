import unittest
import numpy as np
import pyarrow as pa
import pandas as pd
from dataclasses import dataclass

from parquetdb.core.types import (
    PythonObjectArrowType,
    PythonObjectPandasArray,
    PythonObjectPandasDtype
)

@dataclass
class DummyClass:
    x: int
    y: str
    
    def __eq__(self, other):
        if not isinstance(other, DummyClass):
            return False
        return self.x == other.x and self.y == other.y
    
    def __call__(self, x):
        return self.x * x

def dummy_function(x):
    return x ** 2

class TestPythonObjectTypes(unittest.TestCase):
    
    def setUp(self):
        # Create test objects of different types
        self.dummy_obj = DummyClass(x=1, y="test")
        self.dummy_func = dummy_function
        self.lambda_func = lambda x: x + 1
        
        # Create test data with different types of objects
        self.test_objects = [
            self.dummy_obj,
            self.dummy_func,
            self.lambda_func,
            None,
            [1, 2, 3],
            {"a": 1, "b": 2}
        ]
        
        self.test_data={'dummy_obj':[self.dummy_obj for _ in range(5)], 
                        'dummy_func':[self.dummy_func for _ in range(5)], 
                        'lambda_func':[self.lambda_func for _ in range(5)], 
                        'none':[None for _ in range(5)], 
                        'list':[[1, 2, 3] for _ in range(5)], 
                        'dict':[{"a": 1, "b": 2} for _ in range(5)]}
        
        self.test_data_custom={'dummy_obj':PythonObjectPandasArray([self.dummy_obj for _ in range(5)]), 
                        'dummy_func':PythonObjectPandasArray([self.dummy_func for _ in range(5)]), 
                        'lambda_func':PythonObjectPandasArray([self.lambda_func for _ in range(5)]), 
                        'none':[None for _ in range(5)], 
                        'list':[[1, 2, 3] for _ in range(5)], 
                        'dict':[{"a": 1, "b": 2} for _ in range(5)]}

    def test_pandas_array_creation(self):
        # Test creating PythonObjectPandasArray
        
        for key in self.test_data:
            arr = PythonObjectPandasArray(self.test_data[key])
            
            # Verify length and contents
            self.assertEqual(len(arr), len(self.test_data[key]))
            
            self.assertEqual(arr.shape, (len(self.test_data[key]),))        
            # Test that functions still work after storage
            
            if key=='dummy_obj':
                self.assertEqual(arr[0](4), 4)
                self.assertEqual(arr[0].x, 1)
                self.assertEqual(arr[0].y, 'test')
            elif key=='dummy_func':
                self.assertEqual(arr[0](4), 16)
            elif key=='lambda_func':
                self.assertEqual(arr[0](4), 5)
        

    def test_arrow_conversion(self):
        # Test conversion from PythonObjectPandasArray to Arrow Array
       for key in self.test_data:
            arr = PythonObjectPandasArray(self.test_data[key])
            arrow_arr = pa.array(arr, type=PythonObjectArrowType())
            
            self.assertEqual(arrow_arr.type, PythonObjectArrowType())
        
            series=arrow_arr.to_pandas(split_blocks=True, self_destruct=True)
            
            if key=='dummy_obj':
                self.assertEqual(series[0].x, 1)
                self.assertEqual(series[0].y, 'test')
                self.assertEqual(series[2].__class__, type(self.dummy_obj))
            elif key=='dummy_func':
                self.assertEqual(series[0](4), 16)
                self.assertEqual(series[2].__class__, self.test_data[key][2].__class__)
            elif key=='lambda_func':
                self.assertEqual(series[0](4), 5)
                self.assertEqual(series[2].__class__, self.test_data[key][2].__class__)
            elif key=='none':
                self.assertEqual(series[0], None)
                self.assertEqual(series[2], None)
            elif key=='list':
                self.assertEqual(series[0], [1, 2, 3])
                self.assertEqual(series[2], [1, 2, 3])
            elif key=='dict':
                self.assertEqual(series[0], {"a": 1, "b": 2})
                self.assertEqual(series[2], {"a": 1, "b": 2})
    


    def test_dataframe_integration(self):
        # Test integration with pandas DataFrame
        df_initial = pd.DataFrame(self.test_data_custom)
        
        self.assertEqual(df_initial['dummy_obj'].dtype, PythonObjectPandasDtype())
        self.assertEqual(df_initial['dummy_func'].dtype, PythonObjectPandasDtype())
        self.assertEqual(df_initial['lambda_func'].dtype, PythonObjectPandasDtype())
        self.assertEqual(df_initial['none'].dtype, object)
        self.assertEqual(df_initial['list'].dtype, object)
        self.assertEqual(df_initial['dict'].dtype, object)
        
        table=pa.Table.from_pandas(df_initial)
        
        self.assertEqual(table['dummy_obj'].type, PythonObjectArrowType())
        self.assertEqual(table['dummy_func'].type, PythonObjectArrowType())
        self.assertEqual(table['lambda_func'].type, PythonObjectArrowType())
        self.assertEqual(table['none'].type, pa.null())
        self.assertEqual(table['list'].type, pa.list_(pa.int64()))
        self.assertEqual(table['dict'].type, pa.struct([pa.field('a', pa.int64()), pa.field('b', pa.int64())]))
        
        
        df=table.to_pandas(split_blocks=True, self_destruct=True)
        
        self.assertEqual(df['dummy_obj'].dtype, PythonObjectPandasDtype())
        self.assertEqual(df['dummy_func'].dtype, PythonObjectPandasDtype())
        self.assertEqual(df['lambda_func'].dtype, PythonObjectPandasDtype())
        self.assertEqual(df['none'].dtype, object)
        self.assertEqual(df['list'].dtype, object)
        self.assertEqual(df['dict'].dtype, object)
        
        for irow, row in df.iterrows():
            self.assertEqual(row['dummy_obj'], df_initial['dummy_obj'][irow])


    def test_array_operations(self):
        arr = PythonObjectPandasArray(self.test_objects)
        
        # Test slicing
        slice_arr = arr[1:4]
        self.assertEqual(len(slice_arr), 3)
        self.assertEqual(slice_arr[0](2), 4)  # dummy_function(2)
        
        # Test isna
        na_mask = arr.isna()
        self.assertTrue(na_mask[3])  # None should be identified as NA
        self.assertFalse(na_mask[0])  # DummyClass instance should not be NA
        
        # Test iteration
        for i, obj in enumerate(arr):
            self.assertEqual(obj, self.test_objects[i])

    def test_concatenation(self):
        arr1 = PythonObjectPandasArray(self.test_objects[:3])
        arr2 = PythonObjectPandasArray(self.test_objects[3:])
        
        # Test concatenation
        concatenated = PythonObjectPandasArray._concat_same_type([arr1, arr2])
        
        
        df_1=pd.DataFrame(self.test_data_custom)
        df_2=pd.DataFrame(self.test_data_custom)
        df_concat=pd.concat([df_1, df_2])
        
        self.assertEqual(df_concat.shape, (10, 6))
        # # Verify length and contents
        # self.assertEqual(len(concatenated), len(self.test_objects))
        # for i, obj in enumerate(self.test_objects):
        #     if obj is None:
        #         self.assertIsNone(concatenated[i])
        #     else:
        #         self.assertEqual(concatenated[i], obj)

    def test_copy(self):
        arr = self.test_data_custom['dummy_obj']
        arr_copy = arr.copy()
        
        # Verify it's a true copy
        self.assertEqual(len(arr), len(arr_copy))
        for i in range(len(arr)):
            self.assertEqual(arr[i], arr_copy[i])
        
        # Verify that modifying the copy doesn't affect the original
        arr_copy._data[0] = None
        self.assertNotEqual(arr[0], arr_copy[0])

if __name__ == '__main__':
    
    # unittest.TextTestRunner().run(TestPythonObjectTypes('test_pandas_array_creation'))
    # unittest.TextTestRunner().run(TestPythonObjectTypes('test_arrow_conversion'))
    # unittest.TextTestRunner().run(TestPythonObjectTypes('test_dataframe_integration'))
    # unittest.TextTestRunner().run(TestPythonObjectTypes('test_array_operations'))
    # unittest.TextTestRunner().run(TestPythonObjectTypes('test_concatenation'))
    # unittest.TextTestRunner().run(TestPythonObjectTypes('test_copy'))
    unittest.main()
