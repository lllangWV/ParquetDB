import json
import os
import logging
import pandas as pd
from glob import glob
import shutil
import time

from pyarrow import compute as pc
import pyarrow as pa
from parquetdb.core.parquetdb import LoadConfig, NormalizeConfig
from parquetdb.utils.general_utils import timeit
from parquetdb import ParquetDB, config
from parquetdb.utils.external_utils import download_alexandria_3d_database, plot_periodic_table_heatmap

import matplotlib.pyplot as plt

config.logging_config.loggers.parquetdb.level='ERROR'
config.apply()


BENCHMARK_DIR=os.path.join(config.data_dir, 'benchmarks', 'alexandria')
os.makedirs(BENCHMARK_DIR, exist_ok=True)

plt.rcParams['axes.labelsize'] = 18
plt.rcParams['axes.titlesize'] = 18
plt.rcParams['xtick.labelsize'] = 14
plt.rcParams['ytick.labelsize'] = 14

def main():

    base_dir=os.path.join('data','external', 'alexandria', 'AlexandriaDB')
 
    # Here we create a ParquetDatasetDB object to interact with the database
    db=ParquetDB(dataset_name='alexandria_3D_test',dir=base_dir)

    benchmark_dict={
        'create_times':[],
        'json_load_times':[],
        'n_rows_per_file':[],
    }
    

    plot_element_distribution(db, benchmark_dict)
    plot_periodic_table_n_element_heatmap(db, benchmark_dict)
    
    
    plot_material_types(db, benchmark_dict)
    plot_periodic_table_material_type_heatmap(db, benchmark_dict)

    
    
def plot_material_types(db, benchmark_dict):
    # First, define the condition mask for band_gap_ind == 0
    initial_time=time.time()
    
    # Query semiconductors
    start_time=time.time()
    band_gap_dir_in_range = (pc.field("data.band_gap_dir") > 0.1) & (pc.field("data.band_gap_dir") < 3)
    band_gap_ind_in_range = (pc.field("data.band_gap_ind") > 0.1) & (pc.field("data.band_gap_ind") < 3)

    filtered_expr = pc.if_else(
        (pc.field("data.band_gap_ind") != 0) & (pc.field("data.band_gap_ind") < pc.field("data.band_gap_dir")),
        band_gap_ind_in_range,
        band_gap_dir_in_range
    )
    
    table=db.read(columns=['id'], filters=[filtered_expr])
    semiconductor_time = time.time() - start_time
    benchmark_dict['read_filtered_semiconductor_time']=semiconductor_time
    number_of_semiconductors=table.shape[0]
    
    # Query insulators
    start_time=time.time()
    table=db.read(columns=['id'], filters=[pc.field("data.band_gap_dir") > 3])
    insulator_time = time.time() - start_time
    number_of_insulators=table.shape[0]
    
    # Query semimetals
    start_time=time.time()
    band_gap_dir_in_range = (pc.field("data.band_gap_dir") < 0.1)
    band_gap_ind_in_range = (pc.field("data.band_gap_ind") < 0.1)

    filtered_expr = pc.if_else(
        (pc.field("data.band_gap_ind") != 0) & (pc.field("data.band_gap_ind") < pc.field("data.band_gap_dir")),
        band_gap_ind_in_range,
        band_gap_dir_in_range
    )
    
    table=db.read(columns=['id'], filters=[filtered_expr])
    semimetal_time = time.time() - start_time
    number_of_semimetals=table.shape[0]
    
    # Query metals
    start_time=time.time()
    table=db.read(columns=['id'], filters=[pc.field("data.band_gap_dir") == 0])
    metal_time = time.time() - start_time
    number_of_metals=table.shape[0]
    
    # Create bar plot of material types
    plt.figure(figsize=(10, 6))
    materials = ['Semiconductors', 'Small Gap Materials', 'Metals', 'Insulators']
    counts = [number_of_semiconductors, number_of_semimetals, number_of_metals, number_of_insulators]
    band_gap_ranges = ['0.1 < Eg < 3.0', 'Eg < 0.1', 'Eg = 0', 'Eg > 3.0']
    query_times = [semiconductor_time, semimetal_time, metal_time, insulator_time]
    end_time=time.time()
    
    bars = plt.bar(materials, counts, edgecolor='black')
    
    # Add band gap ranges above each bar and counts inside bars
    for i, (bar, range_text, count, query_time) in enumerate(zip(bars, band_gap_ranges, counts, query_times)):
        height = bar.get_height()
        
        
        if i==0:
            
            info_text = f'{range_text}\nCount: {count:.2e}\nQuery Time: {query_time:.2f}s'
            plt.text(bar.get_x() + bar.get_width()/2, height*0.50,
                info_text, ha='center', va='bottom', rotation=0)
             
            #  plt.text(bar.get_x() + bar.get_width()/2, height*0.55,
            #         f'Count: {count:.2e}\nQuery Time: {query_time:.2f}s', ha='center', va='center', rotation=0)
        elif i==1:
            info_text = f'{range_text}\nCount: {count:.2e}\nQuery Time: {query_time:.2f}s'
            plt.text(bar.get_x() + bar.get_width()/2, height*0.50,
                    info_text, ha='center', va='bottom', rotation=0)

        elif i==2:
            info_text = f'{range_text}\nCount: {count:.2e}\nQuery Time: {query_time:.2f}s'
            plt.text(bar.get_x() + bar.get_width()/2, height*0.5,
                    info_text, ha='center', va='bottom', rotation=0)

        elif i==3:
            info_text = f'{range_text}\nCount: {count:.2e}\nQuery Time: {query_time:.2f}s'
            plt.text(bar.get_x() + bar.get_width()/2, height*1.15,
                    info_text, ha='center', va='bottom', rotation=0)
        
    
    
    plt.xlabel('Material Type')
    plt.ylabel('Count (log)')
    plt.yscale('log')
    plt.title(f'Distribution of Material Types')
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig(os.path.join(BENCHMARK_DIR, 'material_types.png'))
    plt.savefig(os.path.join(BENCHMARK_DIR, 'material_types.pdf'))
    plt.show()
    #####################################################################################################
    
def plot_element_distribution(db, benchmark_dict):
    start_time=time.time()
    table=db.read(columns=['data.elements'])
    
    values_array=pc.list_value_length(table['data.elements'])
    length_range = range(1, 8)
    histogram = {length: pc.sum(pc.equal(values_array, length)).as_py() for length in length_range}
    end_time=time.time()
    
    plt.figure(figsize=(10, 6))
    bars = plt.bar(histogram.keys(), histogram.values(), edgecolor='black')
    
    # Add count in scientific notation above each bar
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, height*0.65,
                f'{height:.2e}', ha='center', va='bottom')
    
    plt.xlabel('Number of Elements')
    plt.ylabel('Count (log)')
    plt.yscale('log')
    plt.title(f'Distribution of Number of Elements in Materials \n Time to process: {end_time - start_time:.2f}s')
    plt.grid(True, alpha=0.3)
    plt.xticks(list(length_range))
    plt.savefig(os.path.join(BENCHMARK_DIR, 'element_distribution.png'))
    plt.savefig(os.path.join(BENCHMARK_DIR, 'element_distribution.pdf'))
    plt.show()
    
def filter_list_column(table: pa.Table, column: str, value) -> pa.Table:
    flat_list = pc.list_flatten(table[column])
    flat_list_indices = pc.list_parent_indices(table[column])

    equal_mask = pc.equal(flat_list, value)
    equal_table_indices = pc.filter(flat_list_indices, equal_mask)
    return pc.take(table, equal_table_indices)

def plot_periodic_table_n_element_heatmap(db, benchmark_dict):
    def plot_periodic_table_element_occurrences(db, n_elements, title=None, save_path=None):
        """Plot periodic table heatmap showing element occurrences for systems with specified number of elements.
        
        Parameters
        ----------
        db : ParquetDB
            Database object to query
        n_elements : int
            Number of elements in systems to analyze (e.g. 1 for single element systems)
        title : str, optional
            Custom title for the plot. If None, a default title is used.
        save_path : str, optional
            Path to save the figure. If None, figure is not saved.
        """
        
        if n_elements == 1:
            title = 'Single Element Systems'
        elif n_elements == 2:
            title = 'Binary Element Systems'
        elif n_elements == 3:
            title = 'Ternary Element Systems'
        elif n_elements == 4:
            title = 'Quaternary Element Systems'
        elif n_elements == 5:
            title = 'Quinary Element Systems'
        else:
            title = f'{n_elements}-Element Systems'
            
            
        dirname = os.path.dirname(__file__)
        filter_expression = pc.equal(pc.list_value_length(pc.field('data.elements')), n_elements)
        table = db.read(columns=['data.elements'], filters=[filter_expression])
        element_df = pd.read_csv(os.path.join(dirname, 'imputed_periodic_table_values.csv'))
        symbols = element_df['symbol'].tolist()
        values_element_dict = {symbols[i]: 0 for i in range(len(symbols))}
        
        for key in values_element_dict.keys():
            filtered_table = filter_list_column(table, 'data.elements', key)
            number_of_occurrences = filtered_table.shape[0]
            values_element_dict[key] = number_of_occurrences
        
        values = list(values_element_dict.values())
        element_df['value'] = values
        
        fig, ax = plot_periodic_table_heatmap(element_df, property_name='value')
        
        if title is None:
            title = f'Periodic Table for Occurrences in {title}'
        ax.set_title(title)
        

        fig.savefig(os.path.join(BENCHMARK_DIR,f"periodic_table_{title}.png"))
        fig.savefig(os.path.join(BENCHMARK_DIR,f"periodic_table_{title}.pdf"))
            
        return fig, ax
    
    # Example usage:
    for n_elements in range(1, 6):
        fig, ax = plot_periodic_table_element_occurrences(db, n_elements=n_elements)
    #     # print(filtered_table.shape)
    # # element_symbols=pc.list_value_length(table['data.elements'])
    # print(table.shape)


def plot_periodic_table_material_type_heatmap(db, benchmark_dict):
    def plot_periodic_table_material_type_occurrences(db, material_type, title=None, save_path=None):
        """Plot periodic table heatmap showing element occurrences for systems with specified number of elements.
        
        Parameters
        ----------
        db : ParquetDB
            Database object to query
        material_type : str
            Material type to analyze (e.g. 'semiconductor')
        title : str, optional
            Custom title for the plot. If None, a default title is used.
        save_path : str, optional
            Path to save the figure. If None, figure is not saved.
        """
        
        if material_type == 'semiconductor':
            title = 'Semiconductors'
            band_gap_dir_in_range = (pc.field("data.band_gap_dir") > 0.1) & (pc.field("data.band_gap_dir") < 3)
            band_gap_ind_in_range = (pc.field("data.band_gap_ind") > 0.1) & (pc.field("data.band_gap_ind") < 3)

            filter_expression = pc.if_else(
                (pc.field("data.band_gap_ind") != 0) & (pc.field("data.band_gap_ind") < pc.field("data.band_gap_dir")),
                band_gap_ind_in_range,
                band_gap_dir_in_range
            )
        elif material_type == 'semimetals':
            title = 'Semimetals'
            band_gap_dir_in_range = (pc.field("data.band_gap_dir") < 0.1) 
            band_gap_ind_in_range = (pc.field("data.band_gap_ind") < 0.1)
            
            filter_expression = pc.if_else(
                (pc.field("data.band_gap_ind") != 0) & (pc.field("data.band_gap_ind") < pc.field("data.band_gap_dir")),
                band_gap_ind_in_range,
                band_gap_dir_in_range
            )
            
        elif material_type == 'insulators':
            title = 'Insulators'
            filter_expression = pc.field("data.band_gap_dir") > 3
        elif material_type == 'metals':
            title = 'Metals'
            filter_expression = pc.field("data.band_gap_dir") == 0
        else:
            title = f'{material_type}'
            
            
        dirname = os.path.dirname(__file__)
        table = db.read(columns=['data.elements'], filters=[filter_expression])
        element_df = pd.read_csv(os.path.join(dirname, 'imputed_periodic_table_values.csv'))
        symbols = element_df['symbol'].tolist()
        values_element_dict = {symbols[i]: 0 for i in range(len(symbols))}
        
        
        start_time = time.time()
        for key in values_element_dict.keys():
            filtered_table = filter_list_column(table, 'data.elements', key)
            number_of_occurrences = filtered_table.shape[0]
            values_element_dict[key] = number_of_occurrences
        filtering_time = time.time() - start_time
        
        values = list(values_element_dict.values())
        element_df['value'] = values
        
        fig, ax = plot_periodic_table_heatmap(element_df, property_name='value')
        
        if title is None:
            title = f'Periodic Table for Occurrences in {title}'
        ax.set_title(f'{title} | Filtering Time: {filtering_time:.2f}s')
        
        fig.savefig(os.path.join(BENCHMARK_DIR,f"periodic_table_{title}.png"))
        fig.savefig(os.path.join(BENCHMARK_DIR,f"periodic_table_{title}.pdf"))
            
        return fig, ax
    
    # Example usage:
    material_types = ['semiconductor', 'semimetals', 'insulators', 'metals']
    for material_type in material_types:
        fig, ax = plot_periodic_table_material_type_occurrences(db, material_type=material_type)
    #     # print(filtered_table.shape)
    # # element_symbols=pc.list_value_length(table['data.elements'])
    # print(table.shape)




if __name__ == "__main__":
    
    main()
    
    
    