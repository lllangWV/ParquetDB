import copy
import shutil
import time
import pyarrow as pa
import pyarrow.dataset as ds
import os
import pandas as pd
import numpy as np
import tempfile
# Step 1: Create a dataset (in-memory)
from parquetdb import config

# with tempfile.TemporaryDirectory() as temp_dir:

temp_dir=os.path.join(config.data_dir, 'temp')

if os.path.exists(temp_dir):
    shutil.rmtree(temp_dir)

os.makedirs(temp_dir, exist_ok=True)
try:
    data = {
        "column1": np.random.randint(0, 100, 1000000),
        "column2": np.random.random(1000000),
        "column3": np.random.choice(['A', 'B', 'C'], 1000000)
    }

    # Convert the dictionary to a pandas DataFrame
    df = pd.DataFrame(data)

    # Step 2: Convert the DataFrame to a PyArrow Table
    print(pa.total_allocated_bytes())
    arrow_table = pa.Table.from_pandas(df)
    print(pa.total_allocated_bytes())


    # Step 4: Write the dataset using pyarrow's write_dataset function


    ds.write_dataset(
        data=arrow_table,
        base_dir=temp_dir,
        format="parquet",
        basename_template="part-{i}.parquet"
    )
    print(pa.total_allocated_bytes())
    # Verify files are created in the output directory
    written_files = os.listdir(temp_dir)
    print(written_files)

    print(temp_dir)
    dataset = ds.dataset(temp_dir, format="parquet")

    bytes_allocated = pa.total_allocated_bytes()
    generator = dataset.to_batches(batch_size=100)
    total_bytes=(pa.total_allocated_bytes()-bytes_allocated )/10**6
    print(f"Bytes allocated for batch generator: {total_bytes } MB")
    
    bytes_allocated = pa.total_allocated_bytes()
    record=next(iter(generator))
    print(record.shape)
    total_bytes=(pa.total_allocated_bytes()-bytes_allocated )/10**6
    print(f"Bytes allocated for record batch retrival: {total_bytes } MB")
    
    bytes_allocated = pa.total_allocated_bytes()
    del generator
    time.sleep(0.01)
    total_bytes=(pa.total_allocated_bytes()-bytes_allocated )/10**6
    print(f"Bytes allocated after deleting generator: {total_bytes } MB")
    
    
    bytes_allocated = pa.total_allocated_bytes()
    del record
    time.sleep(0.2)
    total_bytes=(pa.total_allocated_bytes()-bytes_allocated )/10**6
    print(f"Bytes allocated after deleting record: {total_bytes } MB")
    
    
    bytes_allocated = pa.total_allocated_bytes()
    table = dataset.to_table(batch_size=100)
    time.sleep(0.2)
    total_bytes=(pa.total_allocated_bytes()-bytes_allocated )/10**6
    print(f"Bytes allocated after table: {total_bytes } MB")


    bytes_allocated = pa.total_allocated_bytes()
    del table
    time.sleep(2)
    total_bytes=(pa.total_allocated_bytes()-bytes_allocated )/10**6
    print(f"Bytes allocated after deleting table: {total_bytes } MB")
    
    # bytes_allocated = pa.total_allocated_bytes()
    # table = copy.deepcopy(dataset.to_table())

    # print(table)
    # print(f"Bytes allocated after batching table: {pa.total_allocated_bytes()-bytes_allocated }")

except Exception as e:
    print(f"An error occurred: {e}")

time.sleep(0.1)

# finally:
#     # Add a small delay to allow any background processes to complete
#     del generator
#     time.sleep(0.5)


print(pa.total_allocated_bytes()/10**6,'MB')
# dataset = ds.dataset(temp_dir, format="parquet")

# bytes_allocated = pa.total_allocated_bytes()



# except Exception as e:
#     print(e)
#     pass



    # for x in dataset.to_batches(batch_size=10):
    #     print(x)
    # print(generator)
    # print(f"Bytes allocated after batching table: {pa.total_allocated_bytes()-bytes_allocated }")


    # bytes_allocated = pa.total_allocated_bytes()
    # generator = copy.deepcopy(dataset.to_batches(batch_size=10))

    # print(generator)
    # print(f"Bytes allocated after batching table: {pa.total_allocated_bytes()-bytes_allocated }")


    # print(table)
    # print(type(table))
    # print(pa.total_allocated_bytes())
    # for data in dataset:
    #     print(data)
    # new_dataset_dir=os.path.join(temp_dir,'new_dataset')
    # os.makedirs(new_dataset_dir, exist_ok=True)
    # ds.write_dataset(
    #     dataset,
    #     base_dir=new_dataset_dir,
    #     max_rows_per_file=10,
    #     max_rows_per_group=10,
    #     format="parquet",
    #     basename_template="part-{i}.parquet"
    # )

    # written_files = os.listdir(new_dataset_dir)
    # print(written_files)
    

if os.path.exists(temp_dir):
    shutil.rmtree(temp_dir)