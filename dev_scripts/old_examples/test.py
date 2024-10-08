import os
import logging
import json

from parquetdb import ParquetDB

logger=logging.getLogger('parquetdb')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


def test_filter_func(df):
    filtered_df = df[df['band_gap'].apply(lambda x: x == 1.593)]
    return filtered_df
    
     

if __name__ == '__main__':
    save_dir='data/raw/ParquetDB'
    db = ParquetDB(db_path=save_dir)

    ex_json_file_1='data/production/materials_project/json_database/mp-27352.json'
    ex_json_file_2='data/production/materials_project/json_database/mp-1000.json'

    # files=glob('data/production/materials_project/json_database/*.json')


    with open(ex_json_file_1, 'r') as f:
        data_1 = json.load(f)

    with open(ex_json_file_2, 'r') as f:
        data_2 = json.load(f)


    # print(data_1)
    # print('-'*100)
    # print(data_2)

    # json_string_1 = json.dumps(data_1)
    # json_string_2 = json.dumps(data_2)


    data_1['field5']=1
    data_2['field5']=2
    data_list=[]
    for i in range(1000):
        if i%2==0:
            data_list.append(data_1)
        else:
            data_list.append(data_2)
    # db.create(data=data_list, batch_size=100)
    # db.create(data=data_list, table_name='main_test')

    # dataset_dir=os.path.join(save_dir,'datasets','main')
    # for i in range(len(os.listdir(dataset_dir))):
    #     table=pq.read_table(os.path.join(dataset_dir,f'main_{i}.parquet'))
    #     print(table.shape)
    #     # print(table.column_names)


    # filters=[pc.equal(pc.field('band_gap'), 1.593)]
    # table=db.read(table_name='main', ids = [0,1,2,3,4], filters=filters)

    table=db.read(table_name='main_test', columns=['id'])
    df=table.to_pandas()
    print(df.columns)
    print(df.head())
    print(df.tail())

    # print(df['field1'])
    # print(df['field2'])
    # print(df.shape)


    # data_1['field2']=1
    # data_2['field2']=2

    # for i in range(1000):
    #     if i%2==0:
    #         data_list.append(data_1)
    #     else:
    #         data_list.append(data_2)

    # db.create(data=data_list, batch_size=100)

    ################################################################################################
    # Testing Update functionality
    ################################################################################################

    # data_list=[]
    # for i in range(1000):
    #     data={}
    #     data['id']=i
    #     data['field5']=5
    #     data_list.append(data)

    # data_list=[{'id':1, 'field5':5, 'field6':6},
    #            {'id':2, 'field5':5, 'field6':6},
    #            {'id':1000, 'field5':5, 'field8':6},
    #            ]

    # db.update(data_list)

    # table=db.read(ids=[1,2,1000],  table_name='main', columns=['id','field5','field6', 'field8'])
    # print(table)


    ################################################################################################
    # Testing Delete functionality
    ################################################################################################

    # Deleting rows in table
    # db.delete(ids=[1,3,1000,2])
    # db.delete(ids=[2])
    # table=db.read(ids=[1,3,1000,2],  table_name='main', columns=['id','field5','field6', 'field8'])
    # print(table)


    # Trying to delete a row that does not exist
    # db.delete(ids=[10000000])

    ################################################################################################
    # Testing Read functionality
    ################################################################################################
    # # Basic read
    # table=db.read( table_name='main')
    # df=table.to_pandas()
    # print(df.head())
    # print(df.tail())
    # print(df.shape)
    
    # # Testing ids
    # df=db.read(ids=[1,2], table_name='main')
    # print(df.head())
    # print(df.tail())
    # print(df.shape)

    # # # Testing columns include_cols
    # df=db.read(table_name='main', columns=['volume'], include_cols=True)
    # print(df.head())
    # print(df.tail())
    # print(df.shape)

    # # # Testing columns include_cols False
    # df=db.read(table_name='main', columns=['volume'], include_cols=False)
    # print(df.head())
    # print(df.tail())
    # print(df.shape)

    # # # # Testing columns filter_func
    # df=db.read(table_name='main', filter_func=test_filter_func)
    # print(df.head())
    # print(df.tail())
    # print(df.shape)

    # # # # Testing columns filter_args
    # df=db.read(table_name='main', filter_args=('band_gap',  '==', 1.593))
    # print(df.head())
    # print(df.tail())
    # print(df.shape)

    ################################################################################################
    # db._load_data('main')
    # df=db.read(table_name='main', output_format='pandas')

    # # Converting table to dataframe take 0.52 seconds with 1000 rows
    # start_time=time.time()
    # table=pa.Table.from_pandas(df)
    # print("Time taken to convert pandas dataframe to pyarrow table: ", time.time() - start_time)
    
    # # Converting table to dataframe take 0.42 seconds with 1000 rows
    # start_time=time.time()
    # df=table.to_pandas()
    # print("Time taken to convert pyarrow table to pandas dataframe: ", time.time() - start_time)

    # # Converting table to dataset take 0.16 seconds with 1000 rows
    # # table=pa.Table.from_pandas(df)
    # dataset_dir=os.path.join(save_dir,'datasets','main')
    # # start_time=time.time()
    # # ds.write_dataset(table,  
    # #                  base_dir=dataset_dir, 
    # #                  basename_template='main_{i}.parquet',
    # #                  format="parquet",
    # #                 #  partitioning=ds.partitioning(
    # #                 #     pa.schema([table.schema.field(dataset_field)])),
    # #                  max_partitions=1024,
    # #                  max_open_files=1024,
    # #                  max_rows_per_file=200, # Controls the number of rows per parquet file
    # #                  min_rows_per_group=0,
    # #                  max_rows_per_group=200, # This must be less than or equal to max_rows_per_file
    # #                  existing_data_behavior='error', # 'error', 'overwrite_or_ignore', 'delete_matching'
    # #                  )
    # dataset = ds.dataset(dataset_dir, format="parquet")
    # print("Time taken to convert pandas dataframe to pyarrow table and write to parquet: ", time.time() - start_time)


    
    
    
    
    
    
    
    
    
    # df=db.read(table_name='main')
    # print(df.columns)
    # print(df.head())
    # print(df.tail())
    # print(df.shape)
    # print(df['field1'])
    # df=db.read(table_name='main')

    # data=df.iloc[1]['data']
    # lattice=data['structure']['lattice']['matrix']
    # for i in range(3):
    #     print(lattice[i])
    # print(type(lattice))
    # print(lattice.shape)
    # print(data)

    # df=db.read(table_name='main_new')
    # print(df.columns)
    # # print(df.head())
    # # print(df.tail())
    # print(df.shape)

    # # Testing unpacking filter function
    # df=db.read(table_name='main', deserialize_data=True, filter_func=test_filter_func)
    # print(df.head())
    # print(df.tail())
    # print(df.shape)

    # # Testing unpacking filter results
    # df=db.read(table_name='main', deserialize_data=True, filter_func=test_filter_func, unpack_filter_results=True)
    # print(df.head())
    # print(df.tail())
    # print(df.shape)

    # # Testing unpacking filter results
    # df=db.read(table_name='main', columns=['data'], deserialize_data=True, filter_data_func=test_filter_func, unpack_filter_results=True)
    # print(df.head())
    # print(df.tail())
    # print(df.shape)

    # # Testing batch generator
    # batch_df=next(db.get_batch_generator(batch_size=100))
    # print(batch_df.head())
    # print(batch_df.tail())
    # print(batch_df.shape)



    # # Testing update
    # update_data=[{'nsites':100, 'nelements':200}, {'nsites':100, 'nelements':200}]
    # field_data={'field1':[1,2]}

    # Testing updating only data
    # db.update(ids=[1,2], data=update_data, serialize_data=False)

    # # Testing updating only field data
    # db.update(ids=[1,2], field_data=field_data)

    # # Testing updating data and field data
    # db.update(ids=[1,2], data=update_data, field_data=field_data)



    # df=db.read(table_name='main_new', deserialize_data=False)
    # data=df.iloc[0]['data']
    # print(type(data))
    # print(type(data['structure']['lattice']['matrix']))

    # print()

    # for key, value in data.items():
    #     print(key, value)

    # print(data)
    # print(df.head())
    # print(df.tail())
    # print(df.shape)








    #####################################################################################
    # # Testing pyarrow dataset formats 
    #####################################################################################


    # # # Testing output_format
    # table=db.read(table_name='main', output_format='pyarrow')
    # print(table)


    # # # Testing output_format
    # table=db.read(table_name='main', output_format='pyarrow')
    # print(table)
    # # Testing dataset partitioning
    # datasets_dir=os.path.join(save_dir,'datasets')
    # os.makedirs(datasets_dir, exist_ok=True)
    # dataset_field='nelements'
    # dataset_dir=os.path.join(datasets_dir,dataset_field)

    # if not os.path.exists(dataset_dir):
        # https://arrow.apache.org/docs/python/generated/pyarrow.dataset.partitioning.html
        # https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html
        # https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Dataset.html#pyarrow.dataset.Dataset.to_table
        # ds.write_dataset(table, dataset_dir, format="parquet",
        #             partitioning=ds.partitioning(
        #                 pa.schema([table.schema.field(dataset_field)])
        #             ))
        # ds.write_dataset(table,  
        #                 base_dir=dataset_dir, 
        #                 basename_template='main_{i}.parquet',
        #                 format="parquet",
        #                 max_partitions=1024,
        #                 max_open_files=1024,
        #                 max_rows_per_file=200, # Controls the number of rows per parquet file
        #                 min_rows_per_group=0,
        #                 max_rows_per_group=200, # This must be less than or equal to max_rows_per_file
        #                 existing_data_behavior='error', # 'error', 'overwrite_or_ignore', 'delete_matching'
        #                 )
    
    # dataset = ds.dataset(dataset_dir, format="parquet", partitioning=[dataset_field])
    # print(dataset)
    # print(type(dataset))
    # print(dataset.files)
    # # Filtering example
    # table=dataset.to_table(columns=['volume','nelements'], filter=ds.field('nelements') == 2)
    # print(table)


    # # Projecting example
    # projection = {
    #     "volume_float32": ds.field("volume").cast("float32"),
    #     "nelements_int64": ds.field("nelements").cast("int64"),
    # }

    # table=dataset.to_table(columns=projection)
    # print(table)
