import json
import os
import logging
from glob import glob
import shutil
import time

from pyarrow import compute as pc

from parquetdb.utils.general_utils import timeit
from parquetdb import ParquetDB, config
from parquetdb.utils.external_utils import download_alexandria_3d_database

config.logging_config.loggers.parquetdb.level='ERROR'
config.apply()

@timeit
def read_json(json_file):
    with open(json_file, 'r') as f:
        data = json.load(f)
    return data

@timeit
def create_dataset(db, data, **kwargs):
    db.create(data=data, **kwargs)
    
@timeit
def read_dataset(db,**kwargs):
    return db.read(**kwargs)

@timeit
def normalize_dataset(db, **kwargs):
    return db.normalize(**kwargs)

if __name__ == "__main__":
    normalize=True
    from_scratch=False
    base_dir=os.path.join('data','external', 'alexandria', 'AlexandriaDB')
    
    
    if from_scratch and os.path.exists(base_dir):
        print(f"Removing existing directory: {base_dir}")
        shutil.rmtree(base_dir, ignore_errors=True)
    
        
    # Here we download the database and save it to the data directory
    output_dir=os.path.join(config.data_dir, 'external', 'alexandria')
    alexandria_dir = download_alexandria_3d_database(output_dir, n_cores=8)
    
    
    # Here we create a ParquetDatasetDB object to interact with the database
    db=ParquetDB(dataset_name='alexandria_3D',dir=base_dir)

    print(f"Dataset dir: {db.dataset_dir}")
    # Here, we create the dataset inside the database
    start_time = time.time()
    print("The dataset does not exist. Creating it.")
    print('-'*200)
    json_files=glob(os.path.join(alexandria_dir,'*.json'))
    for json_file in json_files:
        data = read_json(json_file)
        
        base_name=os.path.basename(json_file)
        n_materials=len(data['entries'])
        print(f"Processing file: {base_name}")
        print(f"Number of materials: {n_materials}")
        
        try:
            # Since we are importing alot of data it is best
            # to normalize the database afterwards
            create_dataset(db,data['entries'], normalize_dataset=False)
        except Exception as e:
            print(e)
        
        print('-'*200)
        print(f"Time taken to create dataset: {time.time() - start_time}")

    # Now that the data is in the database, we can normalize it. 
    # This means we enfore our parquet files have a certain number of rows. 
    # This improve performance as there are less files to io operations in subsequent steps.
    if normalize:
        print("Normalizing the database")
        normalize_dataset(db,
                        output_format='batch_generator',      # Uses the batch generator to normalize
                        load_kwargs={'batch_readahead': 10,   # Controls the number of batches to load in memory a head of time. This will have impacts on amount of RAM consumed
                                    'fragment_readahead': 2,  # Controls the number of files to load in memory ahead of time. This will have impacts on amount of RAM consumed
                                    },
                        batch_size = 100000,       # Controls the batchsize when to use when normalizing. This will have impacts on amount of RAM consumed
                        max_rows_per_file=500000,  # Controls the max number of rows per parquet file
                        max_rows_per_group=500000) # Controls the max number of rows per group parquet file
    else:
        print("Skipping normalization. Change normalize=True to normalize the database.")
    print("Done with normalization")
    print('-'*200)
    
    
    

    # Here we read a record from the database with id of 0
    table=read_dataset(db, 
                       ids=[0,10,100,1000,10000,100000,1000000], # Controls which rows we want to read
                       output_format='table' # Controls the output format. The options are 'table', 'batch_generator', `dataset`.
                       ) 
    df=table.to_pandas() # Converts the table to a pandas dataframe
    print(df.head())
    print(df.shape)
    
    print(f"Data : {df.iloc[0]['data.spg']}")
    print(list(df.columns))
    print('-'*200)

    
    # Here we read all the records from the database, but only read the 'id' column
    table=read_dataset(db,
                       columns=['id'], 
                       output_format='table')
    print(table.shape)
    print('-'*200)
    
    # With only some subset of columns, we can use built in pyarrow functions to calculate statistics of our column
    table=read_dataset(db,
                       columns=['energy'],
                       output_format='table')
    print(table.shape)
    
    result = pc.min_max(table['energy'])
    # The result will be a struct with 'min' and 'max' fields
    min_value = result['min'].as_py()
    max_value = result['max'].as_py()

    print(f"Min: {min_value}, Max: {max_value}")
    print('-'*200)
    
    
    # Here we filter for rows that have energy above -1.0, but only read the 'id', 'energy' column
    table=read_dataset(db,
                       columns=['id','energy'],
                       filters=[pc.field('energy') > -1.0],
                       output_format='table')
    df=table.to_pandas() # Converts the table to a pandas dataframe
    print(df.head())
    print(df.shape)
    print('-'*200)
    
    
    # Here we filter for rows havea nested subfield we would like to filter by. 
    # In this case I want to filter the 204 space groups
    table=read_dataset(db,
                       columns=['id', 'data.spg'],
                       filters=[pc.field('data.spg') == 204],
                       output_format='table')
    df=table.to_pandas() # Converts the table to a pandas dataframe
    print(df.head())
    print(df.shape)
    print('-'*200)
    
    
    
    # We can also read in batches. This will batch all the rows in 1000 and return tables and 
    # these tables will bet filter by the given filters and columns
    generator=read_dataset(db,
                       output_format='batch_generator',
                       batch_size=1000,
                       load_kwargs={'batch_readahead': 10,
                                    'fragment_readahead': 2,
                                    },
                       columns=['id', 'data.spg'],
                       filters=[pc.field('data.spg') == 204])
    batch_count=0
    num_rows=0
    for table in generator:
        df=table.to_pandas()
        num_rows+=table.num_rows
        batch_count+=1
        # print(f"Batch: {batch_count}, Number of rows: {df.shape[0]}")
        
    print(f"Total number of rows: {num_rows}, Batches: {batch_count}")
    print('-'*200)
    
    
    
    
    