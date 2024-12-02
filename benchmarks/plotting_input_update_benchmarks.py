import os
import shutil
import sys
import time
import random
import string
import itertools

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from mpl_toolkits.axes_grid1.inset_locator import inset_axes
import matplotlib.ticker as ticker

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.fs as fs

from parquetdb import ParquetDB, config



plt.rcParams['axes.labelsize'] = 18
plt.rcParams['axes.titlesize'] = 18
plt.rcParams['xtick.labelsize'] = 14
plt.rcParams['ytick.labelsize'] = 14

db_names=['sqlite','mongodb','parquetdb']
benchmark_dir=os.path.join(config.data_dir, 'benchmarks')
benchmark_dirs=[os.path.join(benchmark_dir, db_name) for db_name in db_names]

benchmark_type='update'


benchmark_files_dict={
    'parquetdb':['parquetdb_input_update_benchmark.csv']
}

line_styles={
    'parquetdb': 'solid'
}
            
def color_diff_log_inset_plot(savefig=None):
    # Initialize a figure and axes
    fig, ax1 = plt.subplots(figsize=(10, 6))


    # Define a color map for different databases
    colors = {
        'sqlite': "#3B9AB2",
        'mongodb': "#EBCC2A",
        'parquetdb': "#F21A00"
    }
    colors = {
        'pylist': "#FF0000",
        'pydict': "#00A08A",
        'pandas': "#5BBCD6",
        'table': "#F2AD00"
    }
    
    

    # Loop through each database benchmark data and plot
    handles=[]
    for db_name, benchmark_files in benchmark_files_dict.items():
        for benchmark_file in benchmark_files:
            basename = os.path.basename(benchmark_file)
            csv_filename = os.path.join(benchmark_dir,db_name, benchmark_file)
            
            df = pd.read_csv(csv_filename)
            groups=df.groupby('input_data_type')
            count=0
            unique_names=set()
            for group_name, group in groups:
                
                print(group_name)
                # Plot create times on the primary y-axis
                
                handle,=ax1.plot(group['n_rows'], group['update_times'], label=group_name, color=colors[group_name], linestyle='solid')
                if group_name not in unique_names:
                    handles.append(handle)
                unique_names.add(group_name)

    # Create twin axis for the secondary y-axis
   

    # Set labels and title
    ax1.set_xlabel('Number of Rows')
    ax1.set_ylabel('Update Times (s)')
    # ax2.set_ylabel('Read Times (s)')
    
    # Ensure both axes have a common scale by linking their limits
    # ax1.set_xscale('log')
    # ax1.set_yscale('log')

    # Color the spines and tick labels
    # ax1.spines['left'].set_color('blue')
    # ax1.tick_params(axis='y', colors='blue')

    # Set the same linestyle and make the spine thicker for visibility
    ax1.spines['left'].set_linestyle('solid')
    ax1.spines['left'].set_linewidth(2.5)  # Increase the line width for visibility


    # Hide the right spine on ax1 and left spine on ax2 to prevent overlap
    # ax1.spines['right'].set_visible(False)


    ax1.tick_params(axis='both', which='major', length=10, width=2, direction='out')

    ax1.grid(True)
    
    scale=36
    ax_inset = inset_axes(ax1, width=f"{scale}%", height=f"{scale}%", loc="upper left", bbox_to_anchor=(0.05, -0.03,1,1), bbox_transform=ax1.transAxes, borderpad=2)

    # Add an inset with log scale
    # ax_inset = inset_axes(ax1, width="30%", height="30%", loc="upper left", bbox_to_anchor=(0,0,1,1))
    # ax_inset = inset_axes(ax1, width="30%", height="30%", bbox_to_anchor=(0,0,1,1))
    ax_inset.grid(True)
    # Plot the same data with log scales
    for db_name, benchmark_files in benchmark_files_dict.items():
        for benchmark_file in benchmark_files:
            basename = os.path.basename(benchmark_file)
            csv_filename = os.path.join(benchmark_dir,db_name, benchmark_file)
            
            df = pd.read_csv(csv_filename)
            groups=df.groupby('input_data_type')
            for group_name, group in groups:
                print(group_name)
                # Plot create times on the primary y-axis
                ax_inset.plot(group['n_rows'], group['update_times'], label=group_name, color=colors[group_name], linestyle='solid')


    # Set log scale for both axes in the inset
    ax_inset.set_xscale('log')
    ax_inset.set_yscale('log')

    nticks = 9
    maj_loc = ticker.LogLocator(numticks=nticks)
    min_loc = ticker.LogLocator(subs='all', numticks=nticks)
    ax_inset.xaxis.set_major_locator(maj_loc)
    ax_inset.xaxis.set_minor_locator(min_loc)
    
    # Set labels for inset plot
    ax_inset.set_xlabel('Number of Rows (log)', fontsize=8)
    ax_inset.set_ylabel('Update Time (log)', fontsize=8, labelpad=-1)
    # ax_inset2.set_ylabel('Read Time (log)', fontsize=8)
    
    
    
    
    
    # Set the same linestyle and make the spine thicker for visibility
    ax_inset.spines['left'].set_linestyle('solid')
    ax_inset.spines['left'].set_linewidth(2.5)  # Increase the line width for visibility


    # Hide the right spine on ax1 and left spine on ax2 to prevent overlap
    # ax_inset.spines['right'].set_visible(False)

    ax_inset.tick_params(axis='both', which='major', length=6, width=1.5, direction='out')
    ax_inset.tick_params(axis='x', which='minor', length=3, width=1, direction='out')
    ax_inset.tick_params(axis='y', which='minor', length=3, width=1, direction='out')

    # lines_1, labels_1 = ax1.get_legend_handles_labels()

    ax1.legend(handles=handles, loc='upper center',  bbox_to_anchor=(0.12, 0,1,1))
    
    ax1.set_title('Data Input Dependence \n Update Benchmark for 100 integer columns')
    plt.tight_layout()
    
    if savefig:
        plt.savefig(savefig)
        return None
    plt.show()
    
    

    
if __name__ == '__main__':
    from parquetdb.utils import matplotlib_utils
    matplotlib_utils.set_palette('Zissou1')

    # color_diff_log_inset_plot()
    color_diff_log_inset_plot(savefig=os.path.join(benchmark_dir, 'benchmark_input_update_times.pdf'))
    color_diff_log_inset_plot(savefig=os.path.join(benchmark_dir, 'benchmark_input_update_times.png'))
    