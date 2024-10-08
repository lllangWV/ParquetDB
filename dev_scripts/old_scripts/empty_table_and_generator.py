import logging
import pyarrow as pa
import pyarrow.dataset as ds




logger=logging.getLogger('parquetdb')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

def create_empty_table(schema: pa.Schema, columns: list = None) -> pa.Table:
    """
    Creates an empty PyArrow table with the same schema as the dataset or specific columns.

    Args:
        schema (pa.Schema): The schema of the dataset to mimic in the empty generator.
        columns (list, optional): List of column names to include in the empty table. Defaults to None.

    Returns:
        pa.Table: An empty PyArrow table with the specified schema.
    """
    # If specific columns are provided, filter the schema to include only those columns
    if columns:
        schema = pa.schema([field for field in schema if field.name in columns])

    # Create an empty table with the derived schema
    empty_table = pa.Table.from_pydict({field.name: [] for field in schema}, schema=schema)

    return empty_table



def create_empty_batch_generator(schema: pa.Schema, columns: list = None):
    """
    Returns an empty generator that yields nothing.

    Args:
        schema (pa.Schema): The schema of the dataset to mimic in the empty generator.
        columns (list, optional): List of column names to include in the empty table. Defaults to None.
    Yields:
        pa.RecordBatch: Empty record batches with the specified schema.
    """
    if columns:
        schema = pa.schema([field for field in schema if field.name in columns])
    yield pa.RecordBatch.from_pydict({field.name: [] for field in schema}, schema=schema)



if __name__ == "__main__":
    
    from parquetdb import ParquetDatasetDB,ParquetDB
    data = [
        {'a': 1, 'b': {'x': 10, 'y': 20}, 'c': {}},
        {'a': 2, 'b': {'x': 30, 'y': 40}, 'c': {}},
    ]
    # print(data)
    

    db.create(data, dataset_name='main')
    # dataset = ds.dataset(db.dataset_dir, format="parquet")
    # schema = dataset.schema
    # Create an empty table# Example usage
    
    # db.create(data)
    
    table=db.read(columns=['a','b'])
    print(table.num_rows)
    table=db.read(columns=['t'])
    print(table.num_rows)
    
    generator=db.read(columns=['a','b'], batch_size=10)
    for table in generator:
        print(table)
        print(table.num_rows)
    generator=db.read(columns=['t'], batch_size=10)
    for table in generator:
        print(table)
        print(table.num_rows)
    
    # print(table.num_rows)
    # df=table.to_pandas()
    # print(df.columns)
    # print(df.head())    
    
    # dataset = ds.dataset(db.dataset_dir, format="parquet")
    # try:
    #     table=dataset.to_table(columns=['t'],filter=None)
    #     df=table.to_pandas()
    # except pa.lib.ArrowInvalid:
    #     print("Exception loading table")
    # print(df.columns)
    # print(df.head())
    # print(df.shape)
    
    # empty_table = create_empty_table(schema=schema, columns=['a','b'])
    # print(empty_table.schema)
    # print(empty_table.num_rows)
    
    # generator=create_empty_batch_generator(schema=schema, columns=['t'])
    # # print(dir(generator))
    # for table in generator:
    #     print(table)
    #     print(table.num_rows)
    # # print(empty_table)
    
    print('reeeee')
