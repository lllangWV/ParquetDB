import random
import os
import time
import tempfile
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from glob import glob
import json
from parquetdb import ParquetDB


def generate_test_data(materials_data, size, num_columns):
    """Generate test data with randomly selected columns from materials_data."""
    # Extract all available columns from the materials data
    all_columns = set()
    for material_data in materials_data:
        all_columns.update(material_data.keys())

    # Randomly select the required number of columns
    selected_columns = random.choices(list(all_columns), k=num_columns)

    # Generate test data by sampling records and including only the selected columns
    test_data = []
    for _ in range(size):
        
        record = random.choices(materials_data,k=1)[0]
        filtered_record = {col: record.get(col, None) for col in selected_columns}
        test_data.append(filtered_record)

    return test_data


def benchmark_read_performance():
    # Create a temporary directory for the database
    save_dir = 'C:/Users/lllang/Desktop/Current_Projects/ParquetDB/data/raw/ParquetDB'
    db = ParquetDB(db_path=save_dir)

    # Define dataset sizes and columns to test
    dataset_sizes = [1000, 5000, 10000, 50000, 100000]  # Adjust sizes as needed
    num_columns_list = [1, 10, 25, 50, 75, 100]  # Range of columns to test
    markers = ['o', 'x', '^', 's', 'D', 'P']  # Different markers for each column count
    colors=['red','blue']
    read_times = []
    write_times = []
    column_counts = []

    read_times = {num_columns: [] for num_columns in num_columns_list}
    write_times = {num_columns: [] for num_columns in num_columns_list}

    materials_dir = "C:/Users/lllang/Desktop/Current_Projects/ParquetDB/data/raw/materials_data"
    files = glob(os.path.join(materials_dir, '*.json'))
    
    # Load materials data from JSON files
    materials_data = []
    for file in files:
        with open(file, 'r') as f:
            materials_data.append(json.load(f))

    try:
        for num_columns in num_columns_list:
            print(f"Testing with {num_columns} columns")

            for size in dataset_sizes:
                print(f"  Dataset size: {size}")

                # Generate test data with the specified number of columns from materials data
                test_data = generate_test_data(materials_data, size, num_columns)

                # Create the table
                table_name = f"test_table_{size}_{num_columns}_columns"
                start_time = time.perf_counter()
                db.create(data=test_data, table_name=table_name)
                end_time = time.perf_counter()
                write_time=end_time - start_time
                write_times[num_columns].append(write_time)

                print(f"  Write time: {write_time} seconds for {num_columns} columns for {size} samples")

                # Measure read time
                start_time = time.perf_counter()
                table = db.read(table_name=table_name, output_format='table')
                end_time = time.perf_counter()
                elapsed_time = end_time - start_time
                read_time=end_time - start_time
                read_times[num_columns].append(read_time)
 

                print(f"  Read time: {read_time} seconds for {num_columns} columns for {size} samples")

    finally:
        pass  # Keep this to ensure clean-up later if needed

    # Plotting the performance
    fig, ax1 = plt.subplots(figsize=(10, 6))

    # Plot read times for different column sizes
    for num_columns, marker in zip(num_columns_list, markers):
        ax1.plot(dataset_sizes, read_times[num_columns],color=colors[1], marker=marker, label=f'Read {num_columns} Columns', linestyle='-')

    

    ax1.set_xlabel('Number of Samples')
    ax1.set_ylabel('Time (seconds)')
    ax1.set_title('ParquetDB Read/Write Performance by Column Count')

    ax2 = ax1.twinx()
    # Plot write times for different column sizes
    for num_columns, marker in zip(num_columns_list, markers):
        ax2.plot(dataset_sizes, write_times[num_columns],color=colors[0], marker=marker, label=f'Write {num_columns} Columns', linestyle='--')
    ax2.set_ylabel('Write Time (seconds)', color='r')
    ax2.tick_params(axis='y', labelcolor='r')


    ax1.grid(True)

    # Add legend
    ax1.legend(loc='upper left', bbox_to_anchor=(1, 1), title="Legend")

    # Save the plot
    plt.tight_layout()
    plt.savefig(os.path.join('data', 'read_write_performance_by_columns_and_samples.png'))

    # Optionally print the results
    print("\nBenchmark Results:")
    for num_columns in num_columns_list:
        for size, read_time, write_time in zip(dataset_sizes, read_times[num_columns], write_times[num_columns]):
            print(f"{size} samples, {num_columns} columns: Read {read_time:.4f} sec, Write {write_time:.4f} sec")



if __name__ == '__main__':
    benchmark_read_performance()
