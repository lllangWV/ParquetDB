import os
import shutil

import pyarrow as pa

from parquetdb import ParquetDB, config
from parquetdb.utils import general_utils

current_data = [
        {'b': {'x': 10, 'y': 20}, 'c': {}, 'e':5},
        {'b': {'x': 30, 'y': 40}, 'c': {}},
    ]

incoming_data = [
        {'id': 2, 'b': {'x': 10, 'y': 20}, 'c': {}},
        {'id': 3, 'b': {'x': 10, 'y': 20}, 'c': {}},
        {'id': 0, 'b': {'y': 50, 'z':{'a':1.1,'b':3.3}}, 'c': {},'d':1, 'e':6},
    ]




current_table=pa.Table.from_pylist(current_data)

incoming_table=pa.Table.from_pylist(incoming_data)



def order_fields_in_table(table, new_schema):