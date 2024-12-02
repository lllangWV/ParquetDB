import os
import json
import time
from glob import glob
from parquetdb import ParquetDB, config
import pyarrow as pa
from jarvis.db.figshare import data
import pandas as pd

config.logging_config.loggers.parquetdb.level = 'ERROR'
config.apply()

def main():
    dataset_name='alex_pbe_3d_all'
    store_dir = os.path.join(config.data_dir, 'external', 'Jarvis', dataset_name)
    os.makedirs(store_dir, exist_ok=True)
    
    
    # ###########################################################################################
    # # Loading dataset
    # ###########################################################################################
    json_file = glob(os.path.join(store_dir, '*.json'))
    if len(json_file) == 0:
        print("No dataset found, downloading...")
        d = download_jarvis_dataset(dataset_name, store_dir) #choose a name of dataset from above
    
    # print("Loading dataset...")
    # json_file = glob(os.path.join(store_dir, '*.json'))
    # with open(json_file[0], 'r') as f:
    #     d = json.load(f)
    
    # print("converting to pandas dataframe to clean dataset")
    # df = pd.DataFrame(d).drop(columns=['id'])


    # # ###########################################################################################
    # # # Creating database
    # # ###########################################################################################
    # print('-'*200)
    # print("Creating database...")
    # print('-'*200)
    # db = ParquetDB(dataset_name=dataset_name, dir=store_dir)
    
    # if db.dataset_exists():
    #     db.drop_dataset()
    
    # db.create(df)
    
    # # Free up memory
    # del df
    # del d
    # # ###########################################################################################
    # # # Schema
    # # ###########################################################################################
    # print('-'*200)
    # print("The schema is:\n")
    # print('-'*200)
    # schema = db.get_schema()
    # # Find the longest field name to align the types
    # max_name_length = max(len(field.name) for field in schema)
    # for field in schema:
    #     print(f'{field.name:<{max_name_length}} | {field.type}')
    # print('-'*200)
    
    # del schema
    
    # # ###########################################################################################
    # # # Full Table read
    # # ###########################################################################################
    # print('-'*200)
    # print("Example of Full Table read")
    # print('-'*200)
    # time.sleep(0.5)
    # print(f"Total allocated bytes: before reading: {pa.total_allocated_bytes() / 10**6} MB")
    # table = db.read(rebuild_nested_struct=True)
    
    # time.sleep(0.5)
    # print(f"Total allocated bytes: after reading: {pa.total_allocated_bytes() / 10**6} MB")
    
    # atoms = table['atoms'].combine_chunks()
    # print(f"length of the atoms column: {len(atoms)}")
    
    # del atoms
    # del table
    
    # # ###########################################################################################
    # # # Batching
    # # ###########################################################################################
    # print('-'*200)
    # print("Example of Batching")
    # print('-'*200)
    # # Wait for the memory to be freed
    # time.sleep(0.5)
    # print(f"Total allocated bytes: before creating generator: {pa.total_allocated_bytes() / 10**6} MB")
    # generator = db.read(load_format='batches', batch_size=10000, rebuild_nested_struct=True)
    # print(f"Total allocated bytes: after creating generator: {pa.total_allocated_bytes() / 10**6} MB")

    # for i,record_batch in enumerate(generator):
    #     # print(record_batch['atoms'].to_pandas())
    #     batch=record_batch
    #     print(f"Bytes allocated for batch {i}: {pa.total_allocated_bytes() / 10**6} MB")
        
    #     del batch


def download_jarvis_dataset(dataset_name: str, store_dir: str, return_data=False):
    os.makedirs(store_dir, exist_ok=True)
    if return_data:
        d = data(dataset_name, store_dir=store_dir)
    else:
        data(dataset_name, store_dir=store_dir)
        return None
    
    # Unzip the downloaded dataset
    zip_files = [f for f in os.listdir(store_dir) if f.endswith('.zip')]
    for zip_file in zip_files:
        import zipfile
        with zipfile.ZipFile(os.path.join(store_dir, zip_file), 'r') as zip_ref:
            zip_ref.extractall(store_dir)
    return d


    
    
if __name__ == "__main__":
    main()
