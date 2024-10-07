from parquetdb.core.parquet_datasetdb import ParquetDatasetDB

db = ParquetDatasetDB(dataset_name='dev', dir='C:/Users/lllang/Desktop/Current_Projects/ParquetDB/data/raw/ParquetDB_Dev', n_cores=1)

data = [
    {'name': 'Judy', 'age': 29}
        ]
db.create(data)

# Add new data with an additional field
new_data = [
    {'name': 'Karl', 'occupation': 'Engineer'}
]
db.create(new_data)

db.create(data)


result = db.read()
df = result.to_pandas()

print(df.columns)
print(df.head())