import json
import os
import logging
from glob import glob

from parquetdb.utils.general_utils import timeit
from parquetdb import ParquetDBManager, ParquetDB, config

# logging_config.logging_config.loggers.parquetdb.level='DEBUG'
# logging_config.apply()







db=ParquetDB(dataset_name='alexandria_3D_test',dir=os.path.join('data', 'ParquetDB'))

@timeit
def read_json(json_file):
    with open(json_file, 'r') as f:
        data = json.load(f)
    return data

# db.drop_dataset()

@timeit
def create_dataset(data,**kwargs):
    db.create(data=data,**kwargs  )
@timeit
def read_dataset(**kwargs):
    return db.read(**kwargs)

def print_dict(data):
    print_str=""
    for key, value in data.items():
        if isinstance(value, dict):
            print_str+="    "
            print_str+=f"{key}: \n"
            print_dict(value)
            continue
        print_str+=f"{key}: {value}\n"
    print(print_str)


    return print_str

@timeit
def main():
    ################################################################################################

    # json_dir=os.path.join('data','external','alexandria','uncompressed')
    # files=glob(os.path.join(json_dir,'*.json'))
    
    # for json_file in files:
    #     data = read_json(json_file)
    #     base_name=os.path.basename(json_file)
    #     print(base_name)
    #     print(len(data['entries']))
        
    #     try:
    #         create_dataset(data['entries'],
    #                     finalize_dataset=False,
    #                     batch_size=100000,
    #                     max_rows_per_file=500000,
    #                     min_rows_per_group=0,
    #                     max_rows_per_group=500000)
    #     except Exception as e:
    #         print(e)
        
    #     print('-'*len(base_name))


    
    
    db.normalize(output_format='batch_generator',
                 load_kwargs={'batch_readahead': 10,
                              'fragment_readahead': 2,
                              },
                 batch_size = 100000, 
                 max_rows_per_file=500000, 
                 max_rows_per_group=500000)
    # print("done")
    # # print(dataset.schema)
    # table=read_dataset(ids=[0],output_format='table')
    # # table=read_dataset()
    # df=table.to_pandas()
    # print(df.head())
    # print(df.shape)
    # print(df.columns)
    # compostion=df.iloc[0]['composition']
    # print(compostion)
    # for name in df.columns:
    #     print('-'*len(name))
    #     print(name)
    #     print(df.iloc[0][name])
        
    # table=read_dataset(columns=['id'],output_format='table')
    
    # print(table.columns)
    # print(table.shape)
        
if __name__ == '__main__':
    main()
    
    