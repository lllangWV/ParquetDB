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

def read_json(json_file):
    with open(json_file, 'r') as f:
        data = json.load(f)
    return data

def create_dataset(db, data, **kwargs):
    db.create(data=data, **kwargs)
    
def read_dataset(db,**kwargs):
    return db.read(**kwargs)


def normalize_dataset(db, **kwargs):
    return db.normalize(**kwargs)

if __name__ == "__main__":
    normalize=True
    from_scratch=False
    base_dir=os.path.join('data','external', 'alexandria', 'AlexandriaDB')
    benchmark_dir=os.path.join(config.data_dir, 'benchmarks', 'alexandria')
    os.makedirs(benchmark_dir, exist_ok=True)
    if from_scratch and os.path.exists(base_dir):
        print(f"Removing existing directory: {base_dir}")
        shutil.rmtree(base_dir, ignore_errors=True)
    
        
    # Here we download the database and save it to the data directory
    output_dir=os.path.join(config.data_dir, 'external', 'alexandria')
    alexandria_dir = download_alexandria_3d_database(output_dir, n_cores=8)
    
    
    # # Here we create a ParquetDatasetDB object to interact with the database
    db=ParquetDB(dataset_name='alexandria_3D',dir=base_dir)

    print(f"Dataset dir: {db.dataset_dir}")
    
    
    
    benchmark_dict={
        'create_times':[],
        'json_load_times':[],
        'n_rows_per_file':[],
    }
    # Here, we create the dataset inside the database
    start_time = time.time()
    print('-'*200)
    if len(os.listdir(db.dataset_dir))==0:
        print("The dataset does not exist. Creating it.")
        json_files=glob(os.path.join(alexandria_dir,'*.json'))
        for json_file in json_files:
            
            start_time = time.time()
            data = read_json(json_file)
            json_load_time = time.time() - start_time
            
            base_name=os.path.basename(json_file)
            n_materials=len(data['entries'])
            print(f"Processing file: {base_name}")
            print(f"Number of materials: {n_materials}")
            
            try:
                # Since we are importing alot of data it is best
                # to normalize the database afterwards
                start_time = time.time()
                create_dataset(db,data['entries'], normalize_dataset=False)
                create_time = time.time() - start_time
                
            except Exception as e:
                print(e)
            
            print('-'*200)
            print(f"Time taken to create dataset: {time.time() - start_time}")
            
            benchmark_dict['create_times'].append(create_time)
            benchmark_dict['json_load_times'].append(json_load_time)
            benchmark_dict['n_rows_per_file'].append(n_materials)

    # Now that the data is in the database, we can normalize it. 
    # This means we enfore our parquet files have a certain number of rows. 
    # This improve performance as there are less files to io operations in subsequent steps.
    if normalize:
        print("Normalizing the database")
        start_time = time.time()
        normalize_dataset(db,
                        load_format='batches',      # Uses the batch generator to normalize
                        load_kwargs={'batch_readahead': 10,   # Controls the number of batches to load in memory a head of time. This will have impacts on amount of RAM consumed
                                    'fragment_readahead': 2,  # Controls the number of files to load in memory ahead of time. This will have impacts on amount of RAM consumed
                                    },
                        batch_size = 100000,       # Controls the batchsize when to use when normalizing. This will have impacts on amount of RAM consumed
                        max_rows_per_file=500000,  # Controls the max number of rows per parquet file
                        max_rows_per_group=500000) # Controls the max number of rows per group parquet file
        benchmark_dict['normalization_time']=time.time() - start_time
    else:
        print("Skipping normalization. Change normalize=True to normalize the database.")
    print("Done with normalization")
    print('-'*200)
    
    
    

    # Here we read a record from the database with id of 0
    start_time = time.time()
    table=read_dataset(db, 
                       ids=[0,10,100,1000,10000,100000,1000000], # Controls which rows we want to read
                       load_format='table' # Controls the output format. The options are 'table', 'batches', `dataset`.
                       )
    read_time = time.time() - start_time
    benchmark_dict['read_ids_time']=read_time
    df=table.to_pandas() # Converts the table to a pandas dataframe
    print(df.head())
    print(df.shape)
    
    print(f"Data : {df.iloc[0]['data.spg']}")
    print(list(df.columns))
    print('-'*200)

    
    # Here we read all the records from the database, but only read the 'id' column
    start_time = time.time()
    table=read_dataset(db,
                       columns=['id'], 
                       load_format='table')
    end_time = time.time() - start_time
    benchmark_dict['read_single_column_time']=end_time
    print(table.shape)
    print('-'*200)
    
    # With only some subset of columns, we can use built in pyarrow functions to calculate statistics of our column
    start_time = time.time()
    table=read_dataset(db,
                       columns=['energy'],
                       load_format='table')
    print(table.shape)
    
    result = pc.min_max(table['energy'])
    # The result will be a struct with 'min' and 'max' fields
    min_value = result['min'].as_py()
    max_value = result['max'].as_py()
    benchmark_dict['min_max_time']=time.time() - start_time

    print(f"Min: {min_value}, Max: {max_value}")
    print('-'*200)
    
    
    # Here we filter for rows that have energy above -1.0, but only read the 'id', 'energy' column
    start_time = time.time()
    table=read_dataset(db,
                       columns=['id','energy'],
                       filters=[pc.field('energy') > -1.0],
                       load_format='table')
    benchmark_dict['read_filtered_energy_above_-1_time']=time.time() - start_time
    df=table.to_pandas() # Converts the table to a pandas dataframe
    print(df.head())
    print(df.shape)
    print('-'*200)
    
    
    # Here we filter for rows havea nested subfield we would like to filter by. 
    # In this case I want to filter the 204 space groups
    start_time = time.time()
    table=read_dataset(db,
                       columns=['id', 'data.spg'],
                       filters=[pc.field('data.spg') == 204],
                       load_format='table')
    benchmark_dict['read_filtered_spg_204_time']=time.time() - start_time
    df=table.to_pandas() # Converts the table to a pandas dataframe
    print(df.head())
    print(df.shape)
    print('-'*200)
    
    
    
    # We can also read in batches. This will batch all the rows in 1000 and return tables and 
    # these tables will bet filter by the given filters and columns
    start_time = time.time()
    generator=read_dataset(db,
                       load_format='batches',
                       batch_size=1000,
                       load_kwargs={'batch_readahead': 10,
                                    'fragment_readahead': 2,
                                    },
                       columns=['id', 'data.spg'],
                       filters=[pc.field('data.spg') == 204])
    benchmark_dict['read_filtered_spg_204_1000_batches_time']=time.time() - start_time
    batch_count=0
    num_rows=0
    for table in generator:
        df=table.to_pandas()
        num_rows+=table.num_rows
        batch_count+=1
        # print(f"Batch: {batch_count}, Number of rows: {df.shape[0]}")
        
    print(f"Total number of rows: {num_rows}, Batches: {batch_count}")
    print('-'*200)
    
    
    # Here we filter for rows havea nested subfield we would like to filter by. 
    # In this case I want to filter the 204 space groups
    start_time = time.time()
    table=read_dataset(db,
                       columns=['id', 'structure.sites'],
                       load_format='table')
    benchmark_dict['read_nested_column_selection_time']=time.time() - start_time
    print(table.shape)
    print(table['structure.sites'].type)
    print(table['structure.sites'].combine_chunks().type)
    

    # By default the database flattens nested structure for storage. 
    # However, we provide an option to rebuild the nested structure. This will create a new dataset in {dataset_name}_nested.
    # After the creation of the new dataset, the query parameters are applied to the new dataset.
    start_time = time.time()
    table=read_dataset(db,
                    columns=['id', 'structure','data'], # Instead of using the flatten syntax, we can use the nested syntax
                    ids=[0],
                    rebuild_nested_struct=True,     # When set to True to rebuild the nested structure
                    rebuild_nested_from_scratch=False,  # When set to True, the nested structure will be rebuilt from scratch
                    load_format='table')
    
    benchmark_dict['read_rebuild_nested_struct_time']=time.time() - start_time
    print(table.shape)
    print(table['data'].type)
    
    try:
        from pymatgen.core.structure import Structure
        
        structure=Structure.from_dict(table['structure'].combine_chunks().to_pylist()[0])
        
        print(structure)
    except Exception as e:
        print(e)
        
    with open(os.path.join(benchmark_dir, 'alexandria_benchmark.json'), 'w') as f:
        json.dump(benchmark_dict, f, indent=4)
    
    
    
    