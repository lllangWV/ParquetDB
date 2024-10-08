from parquetdb import ParquetDatasetDB, logging_config
import pyarrow as pa

from parquetdb.utils import pyarrow_utils

logging_config.logging_config.loggers.parquetdb.level='DEBUG'
logging_config.apply()

db = ParquetDatasetDB(dataset_name='dev', dir='C:/Users/lllang/Desktop/Current_Projects/ParquetDB/data/raw/ParquetDB_Dev', n_cores=1)

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
result = db.read()
df = result.to_pandas()

print(df.columns)
print(df.head())