import os
from parquetdb import ParquetDB, config
import pyarrow as pa

from parquetdb.utils import pyarrow_utils

config.logging_config.loggers.parquetdb.level='DEBUG'
config.apply()

save_dir=os.path.join(config.data_dir, 'raw', 'ParquetDB_Dev')
db = ParquetDB(dataset_name='dev', dir=save_dir, n_cores=1)

data = [
    {'name': 'Judy', 'age': 29}
        ]
# db.create(data)

# # Add new data with an additional field
new_data = [
    {'name': 'Karl', 'occupation': "occupation"}
]
db.create(new_data)

db.create(data)

# table_1=pa.Table.from_pylist(data)
# table_2=pa.Table.from_pylist(new_data)

# schema=pyarrow_utils.merge_schemas(table_1.schema, table_2.schema)
# print(schema)
results = db.read(batch_size=10)
print(results)
for result in results:
    print(result)
# df = result.to_pandas()

# print(df.columns)
# print(df.head())