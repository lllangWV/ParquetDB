import json
import os
import logging
from glob import glob
import shutil
import time

import numpy as np
from pyarrow import compute as pc

from parquetdb.core.parquetdb import LoadConfig, NormalizeConfig
from parquetdb.utils.general_utils import timeit
from parquetdb import ParquetDB, config
from parquetdb.utils.external_utils import download_alexandria_3d_database

import matplotlib.pyplot as plt

config.logging_config.loggers.parquetdb.level='ERROR'
config.apply()

def main():
    
    base_dir=os.path.join('data','external', 'alexandria', 'AlexandriaDB')
    benchmark_dir=os.path.join(config.data_dir, 'benchmarks', 'alexandria')
    
    from_scratch=False

    os.makedirs(benchmark_dir, exist_ok=True)
    
    ################################################################################################
    
    alexandria_dir=download_alexandria_database(base_dir, from_scratch=from_scratch)

    ################################################################################################
    dataset_name = ['alexandria_3D', 'alexandria_3D_test', 'alexandria_3D_benchmark'][0]
    
    db=ParquetDB(dataset_name=dataset_name,dir=base_dir)
    
    benchmark_dict={
        'create_times':[],
        'json_load_times':[],
        'n_rows_per_file':[],
    }
    
    task_benchmark_dict={
        'task_names':[],
        'task_times':[]
    }
        
    ################################################################################################

    # json_load_times, create_times, n_materials_per_file= create_database_if_empty(db, alexandria_dir)
            
    # benchmark_dict['create_times']=create_times
    # benchmark_dict['json_load_times']=json_load_times
    # benchmark_dict['n_rows_per_file']=n_materials_per_file
    
    # with open(os.path.join(benchmark_dir, f"create_load_benchmark_{dataset_name}.json"), 'w') as f:
    #     json.dump(benchmark_dict, f, indent=4)

    ################################################################################################
    # Now that the data is in the database, we can normalize it. 
    # This means we enfore our parquet files have a certain number of rows. 
    # This improve performance as there are less files to io operations in subsequent steps.
    
    start_time = time.time()
    task_name=normalize_dataset(db)
    task_benchmark_dict['task_times'].append(time.time() - start_time)
    task_benchmark_dict['task_names'].append(task_name)

    ################################################################################################
    # Here we read the single column 'id' and track the time taken
    
    start_time=time.time()
    task_name=read_single_column(db)
    task_benchmark_dict['task_times'].append(time.time() - start_time)
    task_benchmark_dict['task_names'].append(task_name)
    
    ################################################################################################
    # Here, we test reading specific ids and track the time taken
    
    start_time=time.time()
    task_name=read_specific_ids(db)
    task_benchmark_dict['task_times'].append(time.time() - start_time)
    task_benchmark_dict['task_names'].append(task_name)
    
    ################################################################################################
    # Here, we read the energy column and calculate the min and max and track the time taken

    start_time=time.time()
    task_name=read_energy_min_max(db)
    task_benchmark_dict['task_times'].append(time.time() - start_time)
    task_benchmark_dict['task_names'].append(task_name)
    
    ################################################################################################
    # Here, we read the records filtered by energy above -1.0 and track the time taken
    
    start_time=time.time()
    task_name=read_filtered_energy_above_minus_one(db)
    task_benchmark_dict['task_times'].append(time.time() - start_time)
    task_benchmark_dict['task_names'].append(task_name)
    
    ################################################################################################
    # Here, we read the records filtered by space group 204 and track the time taken

    start_time=time.time()
    task_name=read_filtered_spg_204(db)
    task_benchmark_dict['task_times'].append(time.time() - start_time)
    task_benchmark_dict['task_names'].append(task_name)
    
    ################################################################################################
    # Here, we read the records filtered by space group 204 in batches and track the time taken
    
    start_time=time.time()
    task_name=read_filtered_spg_batches(db)
    task_benchmark_dict['task_times'].append(time.time() - start_time)
    task_benchmark_dict['task_names'].append(task_name)

    
    ################################################################################################
    # Here, we read the nested column selection and track the time taken
    
    start_time=time.time()
    task_name=read_nested_column_selection(db)
    task_benchmark_dict['task_times'].append(time.time() - start_time)
    task_benchmark_dict['task_names'].append(task_name)

    
    ################################################################################################
    # Here, we read the nested structure into a class and track the time taken
    
    start_time=time.time()
    task_name=read_nested_structure_into_class_from_scratch(db)
    task_benchmark_dict['task_times'].append(time.time() - start_time)
    task_benchmark_dict['task_names'].append(task_name)
        
    
    ################################################################################################
    # Here, we read the nested structure into a class and track the time taken
    
    start_time=time.time()
    task_name=read_nested_structure_into_class(db)
    task_benchmark_dict['task_times'].append(time.time() - start_time)
    task_benchmark_dict['task_names'].append(task_name)
        
    ################################################################################################
    # Here, we update a single record and track the time taken
    
    start_time = time.time()
    task_name=updating_single_record(db)
    task_benchmark_dict['task_times'].append(time.time() - start_time)
    task_benchmark_dict['task_names'].append(task_name)
    
    ################################################################################################
    # Here, we update a single record and track the time taken
    
    task_name,final_time=updating_100000_records(db)
    task_benchmark_dict['task_times'].append(final_time)
    task_benchmark_dict['task_names'].append(task_name)
    
    ################################################################################################
    # Here, we read all the lattice matrix for space group 204 into a numpy array and track the time taken
    
    start_time=time.time()
    task_name=read_lattice_matrix_spg_204(db)
    task_benchmark_dict['task_times'].append(time.time() - start_time)
    task_benchmark_dict['task_names'].append(task_name)
    
    ################################################################################################
    
    with open(os.path.join(benchmark_dir, f"task_benchmark_{dataset_name}.json"), 'w') as f:
        json.dump(task_benchmark_dict, f, indent=4)
    

def download_alexandria_database(base_dir, from_scratch=False):
    print("Starting task: download_alexandria_database")
    if from_scratch and os.path.exists(base_dir):
        print(f"Removing existing directory: {base_dir}")
        shutil.rmtree(base_dir, ignore_errors=True)
        
    # Here we download the database and save it to the data directory
    output_dir=os.path.join(config.data_dir, 'external', 'alexandria')
    alexandria_dir = download_alexandria_3d_database(output_dir, n_cores=8)
    print("Done with task: download_alexandria_database")
    print('-'*200)
    return alexandria_dir
    
def create_database_if_empty(db, alexandria_dir):
    print("Starting task: create_database_if_empty")
    
    start_time = time.time()
    
    json_load_times=[]
    create_times=[]
    n_materials_per_file=[]
    if len(os.listdir(db.dataset_dir))==0:
        print("The dataset does not exist. Creating it.")
        json_files=glob(os.path.join(alexandria_dir,'*.json'))
        for json_file in json_files[:]:
            
            start_time = time.time()
            with open(json_file, 'r') as f:
                data = json.load(f)
            json_load_time = time.time() - start_time
            
            base_name=os.path.basename(json_file)
            n_materials=len(data['entries'])
            print(f"Processing file: {base_name}")
            print(f"Number of materials: {n_materials}")
            try:
                # Since we are importing alot of data it is best
                # to normalize the database afterwards
                start_time = time.time()
                db.create(data['entries'], normalize_dataset=False)
                create_time = time.time() - start_time
                
                create_times.append(create_time)
                n_materials_per_file.append(n_materials)
                json_load_times.append(json_load_time)
                
            except Exception as e:
                print(e)
            
            
            print(f"Time taken to create dataset: {time.time() - start_time}")
            print('-'*100)

    print("Done with task: create_database_if_empty")
    print('-'*200)
    return json_load_times, create_times, n_materials_per_file

def normalize_dataset(db):
    task_name='normalize_dataset'
    print("Starting task: normalize_dataset")
    normalize_config= NormalizeConfig(
        load_format='batches',      # Uses the batch generator to normalize
        batch_readahead=10,         # Controls the number of batches to load in memory a head of time. This will have impacts on amount of RAM consumed
        fragment_readahead=2,       # Controls the number of files to load in memory ahead of time. This will have impacts on amount of RAM consumed
        batch_size = 100000,        # Controls the batchsize when to use when normalizing. This will have impacts on amount of RAM consumed
        max_rows_per_file=500000,   # Controls the max number of rows per parquet file
        max_rows_per_group=500000)  # Controls the max number of rows per group parquet file
    db.normalize(normalize_config=normalize_config)
    print("Done with task: normalize_dataset")
    print('-'*200)
    return task_name

def read_single_column(db):
    task_name='read_single_column'
    print("Starting task: read_single_column")
    table = db.read(
                        columns=['id'],
                        load_format='table')

    print(table.shape)
    print('-'*200)
    return task_name

def read_specific_ids(db):
    task_name='read_specific_ids'
    print("Starting task: read_specific_ids")
    table = db.read(
                    ids=[0,10,100,1000,10000,100000,1000000], # Controls which rows we want to read 
                    load_format='table' # Controls the output format. The options are 'table', 'batches', `dataset`.
                    )

    df = table.to_pandas() # Converts the table to a pandas dataframe
    print(df['id'])
    print(df.head())
    print(df.shape)

    print(f"Data : {df.iloc[0]['data.spg']}")
    print(list(df.columns))
    print("Done with task: read_specific_ids")
    print('-'*200)
    return task_name

def read_energy_min_max(db):
    task_name='read_energy_min_max'
    print("Starting task: read_energy_min_max")
    table = db.read(
                        columns=['energy'],
                        load_format='table')
    print(table.shape)
    
    result = pc.min_max(table['energy'])
    # The result will be a struct with 'min' and 'max' fields
    min_value = result['min'].as_py()
    max_value = result['max'].as_py()

    print(f"Min: {min_value}, Max: {max_value}")
    print("Done with task: read_energy_min_max") 
    print('-'*200)
    return task_name
        
def read_filtered_energy_above_minus_one(db):
    task_name='read_filtered_energy_above_-1'
    
    """Read records filtered by energy above -1.0 and track timing."""
    print("Starting task: read_filtered_energy_above_minus_one")

    table = db.read(
                    columns=['id','energy'],
                    filters=[pc.field('energy') > -1.0],
                    load_format='table')
    
    df = table.to_pandas()  # Converts the table to a pandas dataframe
    print(df.head())
    print(df.shape)
    print("Done with task: read_filtered_energy_above_minus_one")
    print('-'*200)
    return task_name
        
def read_filtered_spg_204(db):
    task_name='read_filtered_spg_204_table'

    print("Starting task: read_filtered_spg_204")
    table = db.read(
                    columns=['id', 'data.spg'],
                    filters=[pc.field('data.spg') == 204],
                    load_format='table')
    
    df = table.to_pandas()  # Converts the table to a pandas dataframe
    print(df.head())
    print(df.shape)
    
    print("Done with task: read_filtered_spg_204")
    print('-'*200)
    return task_name

def read_filtered_spg_batches(db):
    task_name='read_filtered_spg_batches'
    print("Starting task: read_filtered_spg_batches")
    generator = db.read(
                        load_format='batches', 
                        batch_size=1000,
                        load_config=LoadConfig(batch_readahead=10,
                                                fragment_readahead=2,
                                                fragment_scan_options=None,
                                                use_threads=True,
                                                memory_pool=None),
                        columns=['id', 'data.spg'],
                        filters=[pc.field('data.spg') == 204])

    batch_count = 0
    num_rows = 0
    for table in generator:
        df = table.to_pandas()
        num_rows += table.num_rows
        batch_count += 1
    print(f"Total number of rows: {num_rows}, Batches: {batch_count}")
    print("Done with task: read_filtered_spg_batches")
    print('-'*200)
    return task_name

def read_nested_column_selection(db):
    task_name='read_nested_column_selection'
    
    print("Starting task: read_nested_column_selection")
    table = db.read(
                    columns=['id', 'structure.sites'],
                    load_format='table')

    print(table.shape)
    print(table['structure.sites'].type)
    print(table['structure.sites'].combine_chunks().type)
    print("Done with task: read_nested_column_selection")
    print('-'*200)
    return task_name

def read_nested_structure_into_class_from_scratch(db):
    # By default the database flattens nested structure for storage. 
    # However, we provide an option to rebuild the nested structure. This will create a new dataset in {dataset_name}_nested.
    # After the creation of the new dataset, the query parameters are applied to the new dataset.
    task_name='read_nested_structure_into_class_from_scratch'
    
    print("Starting task: read_nested_structure_into_class_from_scratch")
    table = db.read(columns=['id', 'structure','data'], # Instead of using the flatten syntax, we can use the nested syntax
                    ids=[0,1000000],
                    load_format='table',
                    rebuild_nested_struct=True,         # When set to True to rebuild the nested structure
                    rebuild_nested_from_scratch=True,  # When set to True, the nested structure will be rebuilt from scratch
                    normalize_config=NormalizeConfig(
                        load_format='batches',
                        batch_readahead=2,
                        fragment_readahead=2,
                        batch_size=50000,
                        max_rows_per_file=500000,
                        min_rows_per_group=0,
                        max_rows_per_group=500000
                    )
                    )

    print(table.shape)
    print(table['data'].type)
    
    print("structure type")
    print(table['structure'].type)
    try:
        from pymatgen.core.structure import Structure
        
        structure=Structure.from_dict(table['structure'].combine_chunks().to_pylist()[0])
        
        print(structure)
    except Exception as e:
        print(e)
    print("Done with task: read_nested_structure_into_class_from_scratch")
    print('-'*200)
    return task_name


def read_nested_structure_into_class(db):
    # By default the database flattens nested structure for storage. 
    # However, we provide an option to rebuild the nested structure. This will create a new dataset in {dataset_name}_nested.
    # After the creation of the new dataset, the query parameters are applied to the new dataset.
    task_name='read_nested_structure_into_class'
    
    print("Starting task: read_nested_structure_into_class")
    table = db.read(columns=['id', 'structure','data'], # Instead of using the flatten syntax, we can use the nested syntax
                    ids=[0,1000000],
                    load_format='table',
                    rebuild_nested_struct=True,         # When set to True to rebuild the nested structure
                    rebuild_nested_from_scratch=False,  # When set to True, the nested structure will be rebuilt from scratch
                    normalize_config=NormalizeConfig(
                        load_format='batches',
                        batch_readahead=2,
                        fragment_readahead=2,
                        batch_size=50000,
                        max_rows_per_file=500000,
                        min_rows_per_group=0,
                        max_rows_per_group=500000
                    )
                    )

    print(table.shape)
    print(table['data'].type)
    
    print("structure type")
    print(table['structure'].type)
    try:
        from pymatgen.core.structure import Structure
        
        structure=Structure.from_dict(table['structure'].combine_chunks().to_pylist()[0])
        
        print(structure)
    except Exception as e:
        print(e)
    print("Done with task: read_nested_structure_into_class")
    print('-'*200)
    return task_name
    
def updating_single_record(db):
    print("Starting task: updating_single_record")
    task_name='updating_single_record'
    
    # table=db.read(ids=[0])
    # df=table.to_pandas()
    # print(df['data.spg'])
    start_time = time.time()
    db.update([{'id':0, 'data.spg':210}], 
            normalize_config=NormalizeConfig(
                load_format='batches',      # Uses the batch generator to normalize
                batch_readahead=10,   # Controls the number of batches to load in memory a head of time. This will have impacts on amount of RAM consumed
                fragment_readahead=2,  # Controls the number of files to load in memory ahead of time. This will have impacts on amount of RAM consumed
                batch_size = 100000,       # Controls the batchsize when to use when normalizing. This will have impacts on amount of RAM consumed
                max_rows_per_file=500000,  # Controls the max number of rows per parquet file                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
                max_rows_per_group=500000) # Controls the max number of rows per group parquet file
          )
    print(f"Time taken to update: {time.time() - start_time}")
    table=db.read(ids=[0])
    print(table.shape)
    print("Done with task: updating_dataset")
    print('-'*200)
    return task_name
    
def updating_100000_records(db):
    print("Starting task: updating_100000_records")
    task_name='updating_100000_records'
    
    # Read first 100000 records
    table = db.read(columns=['id'])
    # df = table.to_pandas()
    # print("Original spg values for first few records:")
    # print(df['data.spg'].head())

    # Create list of 100000 updates
    updates = [{'id': np.random.randint(0, table.num_rows), 'data.spg': 210} for _ in range(100000)]
    start_time = time.time()
    db.update(updates,
            normalize_config=NormalizeConfig(
                load_format='batches',      # Uses the batch generator to normalize
                batch_readahead=10,   # Controls the number of batches to load in memory a head of time. This will have impacts on amount of RAM consumed
                fragment_readahead=2,  # Controls the number of files to load in memory ahead of time. This will have impacts on amount of RAM consumed
                batch_size = 100000,       # Controls the batchsize when to use when normalizing. This will have impacts on amount of RAM consumed
                max_rows_per_file=500000,  # Controls the max number of rows per parquet file
                max_rows_per_group=500000) # Controls the max number of rows per group parquet file
          )
    final_time = time.time() - start_time
    # Verify updates
    print("Updated spg values for first few records:")
    print(table.shape)
    # print(df['data.spg'].head())
    print("Done with task: updating_100000_records")
    print('-'*200)
    return task_name,final_time
    
def read_lattice_matrix_spg_204(db):
    task_name='read_lattice_matrix_spg_204'
    
    print("Starting task: read_lattice_matrix_spg_204")
    
    table = db.read(columns=['structure.lattice.matrix'], 
                    filters=[pc.field('data.spg') == 204])
    lattice = table['structure.lattice.matrix'].combine_chunks().to_numpy_ndarray()
    print(lattice.shape)
    print("Done with task: read_lattice_matrix_spg_204")
    print('-'*200)
    return task_name


    
if __name__ == "__main__":
    main()
