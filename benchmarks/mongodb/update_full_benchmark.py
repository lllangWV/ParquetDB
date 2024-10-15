import os
import random
import time
from pymongo import MongoClient, UpdateOne, UpdateMany
import pandas as pd
from parquetdb import config

def generate_data(num_rows, n_columns=100):
    data = []
    for i in range(num_rows):
        doc = {'id': i}  # Unique identifier for each document
        doc.update({f'col{j}': random.randint(0, 1000000) for j in range(n_columns)})
        data.append(doc)
    return data

def remove_db(client, db_name):
    client.drop_database(db_name)

def benchmark_read_write_update(num_rows, client, db_name='benchmark'):
    # Remove existing database to start fresh
    remove_db(client, db_name)
    
    # Generate initial data
    data = generate_data(num_rows)
    
    # Start timing insertion
    start_time = time.time()
    
    # Insert data
    db = client[db_name]
    collection = db['test_collection']
    collection.insert_many(data)
    
    insert_time = time.time() - start_time
    data = None  # Release memory
    
    
    # Create an index on the 'id' field (index creation time not included in update time)
    collection.create_index('id', unique=True)
    ################################################################################################

    # Start timing reading
    start_time = time.time()
    
    # Execute read query
    cursor = collection.find({})
    list(cursor)  # Materialize the cursor
    
    read_time = time.time() - start_time

    # Prepare update data with the same 'id's
    update_data = generate_data(num_rows)
    
    # Prepare bulk update operations
    update_operations = []
    for doc in update_data:
        doc_id = doc['id']
        update_fields = {k: v for k, v in doc.items() if k != 'id'}
        update_operations.append(
            UpdateOne({'id': doc_id}, {'$set': update_fields})
        )

        
    # Measure update time
    start_time = time.time()
    
    # Perform bulk update operations
    if update_operations:
        collection.bulk_write(update_operations)
    
    update_time = time.time() - start_time

    return insert_time, read_time, update_time

if __name__ == '__main__':
    save_dir = os.path.join(config.data_dir, 'benchmarks', 'mongodb')
    os.makedirs(save_dir, exist_ok=True)
    
    # Connect to MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    
    row_counts = [1, 10, 100, 1000, 10000, 100000, 1000000]
    insertion_times = []
    reading_times = []
    update_times = []

    results = {
        'create_times': [],
        'read_times': [],
        'update_times': [],
        'n_rows': []
    }

    for num_rows in row_counts:
        print(f'Benchmarking {num_rows} rows...')
        
        insert_time, read_time, update_time = benchmark_read_write_update(num_rows, client)
        insertion_times.append(insert_time)
        reading_times.append(read_time)
        update_times.append(update_time)
        print(f'Insertion Time for {num_rows} rows: {insert_time:.4f} seconds')
        print(f'Reading Time for {num_rows} rows: {read_time:.4f} seconds')
        print(f'Update Time for {num_rows} rows: {update_time:.4f} seconds')
        print('---')
        
    results['create_times'] = insertion_times
    results['read_times'] = reading_times
    results['update_times'] = update_times
    results['n_rows'] = row_counts

    df = pd.DataFrame(results)
    df.to_csv(os.path.join(save_dir, 'mongodb_update_benchmark.csv'), index=False)

    # Close the MongoDB connection
    client.close()