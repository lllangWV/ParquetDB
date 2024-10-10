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

combined_template = current_data + incoming_data

current_data = general_utils.generate_similar_data(current_data, num_entries=10000)
incoming_data = general_utils.generate_similar_data(combined_template, num_entries=1000)


for x in incoming_data:
    print(x)

save_dir = f"{config.data_dir}/temp/ParquetDB"

if os.path.exists(save_dir):
    shutil.rmtree(save_dir)
os.makedirs(save_dir, exist_ok=True)
# temp_dir = tempfile.mkdtemp()


config.logging_config.loggers.parquetdb.level='INFO'
config.apply()

db = ParquetDB(dataset_name='dev', dir=save_dir, n_cores=1)


db.create(current_data)



table=db.read()

print(f"Table shape: {table.shape}")
print(f"DataFrame:\n{table.to_pandas()}")


db.update(incoming_data)


table=db.read()

print(f"Table shape: {table.shape}")
print(f"DataFrame:\n{table.to_pandas()}")


