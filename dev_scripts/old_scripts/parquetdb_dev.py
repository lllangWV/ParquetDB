import logging
import random
import os
import time
import tempfile
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from glob import glob
import json

import pyarrow as pa

from parquetdb import ParquetDB

logger=logging.getLogger('parquetdb')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


materials_dir = "C:/Users/lllang/Desktop/Current_Projects/ParquetDB/data/raw/materials_data"
files = glob(os.path.join(materials_dir, '*.json'))

# Load materials data from JSON files
materials_data = []
for file in files:
    with open(file, 'r') as f:
        materials_data.append(json.load(f))


save_dir = 'C:/Users/lllang/Desktop/Current_Projects/ParquetDB/data/raw/ParquetDB'
db = ParquetDB(db_path=save_dir)
db.drop_table('main_test')

# data_list=[]
# for i in range(1000):
#     data = random.choices(materials_data,k=1)[0]
#     data_list.append(data)
# db.create(data=data_list, table_name='main_test', max_rows_per_file=100, min_rows_per_group=0, max_rows_per_group=100)

data_list=[]
for i in range(100):
    data = materials_data[0]
    data_list.append(data)

db.create(data=data_list, table_name='main_test', max_rows_per_file=100, min_rows_per_group=0, max_rows_per_group=100)

data_list=[]
for i in range(100):
    data = materials_data[1]
    data_list.append(data)

db.create(data=data_list, table_name='main_test', max_rows_per_file=100, min_rows_per_group=0, max_rows_per_group=100)


data_list=[]
for i in range(100):
    data = materials_data[2]
    data_list.append(data)

db.create(data=data_list, table_name='main_test', max_rows_per_file=100, min_rows_per_group=0, max_rows_per_group=100)



table=db.read(table_name='main_test')

# schema=db.get_schema('main_test')
# # print(schema)

# print(detect_struct_types(schema))


df=table.to_pandas()
print(df.columns)
print(df.head())
print(df.tail())
print(df.shape)
print(df['composition'].iloc[-1])
print(df['structure'].iloc[-1])


db.update([{'id':299, 'nsites': 100, 'composition': {'Ca':1.0}, 'structure': {'lattice': {'matrix': [[1, 0, 0], [0, 1, 0], [0, 0, 1]]}}}], table_name='main_test')
table=db.read(table_name='main_test')
df=table.to_pandas()
print(df.columns)
print(df.head())
print(df.tail())
print(df.shape)

print(df['structure'].iloc[-1])
print(df['composition'].iloc[-1])
print(df['nsites'].iloc[-1])
# print(df['field1'])
# print(df['field2'])
# print(df.shape)


################################################################################################
# Testing Update functionality
################################################################################################

# data_list=[]
# for i in range(1000):
#     data={}
#     data['id']=i
#     data['field5']=5
#     data_list.append(data)

# data_list=[{'id':1, 'field5':5, 'field6':6},
#            {'id':2, 'field5':5, 'field6':6},
#            {'id':1000, 'field5':5, 'field8':6},
#            ]

# db.update(data_list)

# table=db.read(ids=[1,2,1000],  table_name='main', columns=['id','field5','field6', 'field8'])
# print(table)


################################################################################################
# Testing Delete functionality
################################################################################################

# Deleting rows in table
# db.delete(ids=[1,3,1000,2])
# db.delete(ids=[2])
# table=db.read(ids=[1,3,1000,2],  table_name='main', columns=['id','field5','field6', 'field8'])
# print(table)


# Trying to delete a row that does not exist
# db.delete(ids=[10000000])

################################################################################################
# Testing Read functionality
################################################################################################
# # Basic read
# table=db.read( table_name='main')
# df=table.to_pandas()
# print(df.head())
# print(df.tail())
# print(df.shape)

# # Testing ids
# df=db.read(ids=[1,2], table_name='main')
# print(df.head())
# print(df.tail())
# print(df.shape)

# # # Testing columns include_cols
# df=db.read(table_name='main', columns=['volume'], include_cols=True)
# print(df.head())
# print(df.tail())
# print(df.shape)

# # # Testing columns include_cols False
# df=db.read(table_name='main', columns=['volume'], include_cols=False)
# print(df.head())
# print(df.tail())
# print(df.shape)

# # # # Testing columns filter_func
# df=db.read(table_name='main', filter_func=test_filter_func)
# print(df.head())
# print(df.tail())
# print(df.shape)

# # # # Testing columns filter_args
# df=db.read(table_name='main', filter_args=('band_gap',  '==', 1.593))
# print(df.head())
# print(df.tail())
# print(df.shape)

########################################################################################