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

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.fs as fs

from parquetdb import ParquetDB, config
import matplotlib.ticker as ticker

plt.rcParams['axes.labelsize'] = 18
plt.rcParams['axes.titlesize'] = 18
plt.rcParams['xtick.labelsize'] = 14
plt.rcParams['ytick.labelsize'] = 14


db_names=['sqlite','mongodb','parquetdb']
banchmark_dir=os.path.join(config.data_dir, 'benchmarks')
benchmark_dirs=[os.path.join(banchmark_dir, db_name) for db_name in db_names]





def line_style_diff():
    # Initialize a figure and axes
    fig, ax1 = plt.subplots(figsize=(10, 6))


    colors = {
        'create_times': 'blue',
        'read_times': 'red',
    }


    line_styles = {
        'sqlite': 'dotted',
        'mongodb': 'dashdot',
        'parquetdb': 'solid'
    }


    for benchmark_dir in benchmark_dirs:
        basename = os.path.basename(benchmark_dir)
        csv_filename = os.path.join(benchmark_dir, f'{basename}_benchmark.csv')
        
        df = pd.read_csv(csv_filename)

        # Plot create times on the primary y-axis
        ax1.plot(df['n_rows'], df['create_times'], label=f'{basename} create', color=colors['create_times'], linestyle=line_styles[basename])

        # Plot read times on the secondary y-axis
    ax2 = ax1.twinx()

    # ax2.set_xscale('log')
    # ax1.set_yscale('log')
    # ax2.set_yscale('log')
    for benchmark_dir in benchmark_dirs:
        basename = os.path.basename(benchmark_dir)
        csv_filename = os.path.join(benchmark_dir, f'{basename}_benchmark.csv')
        df = pd.read_csv(csv_filename)
        ax2.plot(df['n_rows'], df['read_times'], label=f'{basename} read', color=colors['read_times'], linestyle=line_styles[basename])

    # Set labels and title
    ax1.set_xlabel('Number of Rows')
    ax1.set_ylabel('Create Times (s)', color='blue')
    ax2.set_ylabel('Read Times (s)', color='red')

    # Setting scales
    # ax1.set_xscale('log')
    # ax1.set_yscale('log')
    # ax2.set_yscale('log')


    # Color the spines and tick labels
    ax1.spines['left'].set_color(colors['create_times'])
    ax1.tick_params(axis='y', colors=colors['create_times'])
    ax2.spines['right'].set_color(colors['read_times'])
    ax2.tick_params(axis='y', colors=colors['read_times'])
    
    ax1.grid()

    plt.title('Benchmark Create and Read Times for Different Databases')

    # Combine legends from both axes
    lines_1, labels_1 = ax1.get_legend_handles_labels()
    lines_2, labels_2 = ax2.get_legend_handles_labels()
    ax1.legend(lines_1 + lines_2, labels_1 + labels_2, loc='upper left')

    plt.tight_layout()
    plt.show()
    
    
    
def color_diff_plot():
    # Initialize a figure and axes
    fig, ax1 = plt.subplots(figsize=(10, 6))


    # Define a color map for different databases
    colors = {
        'sqlite': 'blue',
        'mongodb': 'green',
        'parquetdb': 'red'
    }
    
    line_styles = {
        'create_times': 'solid',
        'read_times': 'dashed',
    }

    # Loop through each database benchmark data and plot
    for benchmark_dir in benchmark_dirs:
        basename = os.path.basename(benchmark_dir)
        csv_filename = os.path.join(benchmark_dir, f'{basename}_benchmark.csv')
        
        df = pd.read_csv(csv_filename)

        # Plot create times on the primary y-axis
        ax1.plot(df['n_rows'], df['create_times'], label=f'{basename} create', color=colors[basename], linestyle=line_styles['create_times'])

    # Create twin axis for the secondary y-axis
    ax2 = ax1.twinx()

    for benchmark_dir in benchmark_dirs:
        basename = os.path.basename(benchmark_dir)
        csv_filename = os.path.join(benchmark_dir, f'{basename}_benchmark.csv')
        
        df = pd.read_csv(csv_filename)

        # Plot read times on the secondary y-axis
        ax2.plot(df['n_rows'], df['read_times'], label=f'{basename} read', color=colors[basename], linestyle=line_styles['read_times'])

    # Set labels and title
    ax1.set_xlabel('Number of Rows')
    ax1.set_ylabel('Create Times (s)')
    ax2.set_ylabel('Read Times (s)')
    plt.title('Benchmark Create and Read Times for Different Databases')

    # Ensure both axes have a common scale by linking their limits
    # ax2.set_ylim(ax1.get_ylim())
    # ax1.set_xscale('log')
    # ax1.set_yscale('log')
    # ax2.set_yscale('log')

    # Color the spines and tick labels
    # ax1.spines['left'].set_color('blue')
    # ax1.tick_params(axis='y', colors='blue')
    # ax2.spines['right'].set_color('red')
    # ax2.tick_params(axis='y', colors='red')

    # Set the same linestyle and make the spine thicker for visibility
    ax1.spines['left'].set_linestyle(line_styles['create_times'])
    ax1.spines['left'].set_linewidth(2.5)  # Increase the line width for visibility
    ax2.spines['right'].set_linestyle(line_styles['read_times'])
    ax2.spines['right'].set_linewidth(2.5)  # Increase the line width for visibility

    # Hide the right spine on ax1 and left spine on ax2 to prevent overlap
    ax1.spines['right'].set_visible(False)
    ax2.spines['left'].set_visible(False)

    # Combine legends from both axes
    lines_1, labels_1 = ax1.get_legend_handles_labels()
    lines_2, labels_2 = ax2.get_legend_handles_labels()
    ax1.legend(lines_1 + lines_2, labels_1 + labels_2, loc='upper left')
    
    ax1.grid(True)
    

    plt.tight_layout()
    plt.show()
    

def color_diff_log_two_axes_plot(savefig=None):
    # Initialize a figure and axes
    fig, ax1 = plt.subplots(ncols=2, figsize=(10, 6))


    # Define a color map for different databases
    colors = {
        'sqlite': "#3B9AB2",
        'mongodb': "#EBCC2A",
        'parquetdb': "#F21A00"
    }
    
    line_styles = {
        'create_times': 'solid',
        'read_times': 'dashed',
    }

    # Loop through each database benchmark data and plot
    for benchmark_dir in benchmark_dirs:
        basename = os.path.basename(benchmark_dir)
        csv_filename = os.path.join(benchmark_dir, f'{basename}_benchmark.csv')
        
        df = pd.read_csv(csv_filename)

        # Plot create times on the primary y-axis
        ax1[0].plot(df['n_rows'], df['create_times'], label=f'{basename} create', color=colors[basename], linestyle=line_styles['create_times'])

    # Create twin axis for the secondary y-axis
    ax2 = ax1[0].twinx()

    for benchmark_dir in benchmark_dirs:
        basename = os.path.basename(benchmark_dir)
        csv_filename = os.path.join(benchmark_dir, f'{basename}_benchmark.csv')
        
        df = pd.read_csv(csv_filename)

        # Plot read times on the secondary y-axis
        ax2.plot(df['n_rows'], df['read_times'], label=f'{basename} read', color=colors[basename], linestyle=line_styles['read_times'])

    # Set labels and title
    ax1[0].set_xlabel('Number of Rows')
    ax1[0].set_ylabel('Create Times (s)')
    ax2.set_ylabel('Read Times (s)')
    plt.title('Benchmark Create and Read Times for Different Databases')

    # Ensure both axes have a common scale by linking their limits
    # ax2.set_ylim(ax1[0].get_ylim())
    # ax1[0].set_xscale('log')
    # ax1[0].set_yscale('log')
    # ax2.set_yscale('log')

    # Color the spines and tick labels
    # ax1[0].spines['left'].set_color('blue')
    # ax1[0].tick_params(axis='y', colors='blue')
    # ax2.spines['right'].set_color('red')
    # ax2.tick_params(axis='y', colors='red')

    # Set the same linestyle and make the spine thicker for visibility
    ax1[0].spines['left'].set_linestyle(line_styles['create_times'])
    ax1[0].spines['left'].set_linewidth(2.5)  # Increase the line width for visibility
    ax2.spines['right'].set_linestyle(line_styles['read_times'])
    ax2.spines['right'].set_linewidth(2.5)  # Increase the line width for visibility


    ax1[0].tick_params(axis='both', which='major', length=10, width=2, direction='out')
    ax2.tick_params(axis='both', which='major', length=10, width=2, direction='out')

    # Hide the right spine on ax1[0] and left spine on ax2 to prevent overlap
    ax1[0].spines['right'].set_visible(False)
    ax2.spines['left'].set_visible(False)

    # Combine legends from both axes
    lines_1, labels_1 = ax1[0].get_legend_handles_labels()
    lines_2, labels_2 = ax2.get_legend_handles_labels()
    ax1[0].legend(lines_1 + lines_2, labels_1 + labels_2, loc='upper left')
    
    ax1[0].grid(True)
    
    #######################################################################################################################


    # Loop through each database benchmark data and plot
    for benchmark_dir in benchmark_dirs:
        basename = os.path.basename(benchmark_dir)
        csv_filename = os.path.join(benchmark_dir, f'{basename}_benchmark.csv')
        
        df = pd.read_csv(csv_filename)

        # Plot create times on the primary y-axis
        ax1[1].plot(df['n_rows'], df['create_times'], label=f'{basename} create', color=colors[basename], linestyle=line_styles['create_times'])

    # Create twin axis for the secondary y-axis
    ax2 = ax1[1].twinx()

    for benchmark_dir in benchmark_dirs:
        basename = os.path.basename(benchmark_dir)
        csv_filename = os.path.join(benchmark_dir, f'{basename}_benchmark.csv')
        
        df = pd.read_csv(csv_filename)

        # Plot read times on the secondary y-axis
        ax2.plot(df['n_rows'], df['read_times'], label=f'{basename} read', color=colors[basename], linestyle=line_styles['read_times'])

    # Set labels and title
    ax1[1].set_xlabel('Number of Rows')
    ax1[1].set_ylabel('Create Times (s)')
    ax2.set_ylabel('Read Times (s)')
    plt.title('Benchmark Create and Read Times for Different Databases')

    # Ensure both axes have a common scale by linking their limits
    # ax2.set_ylim(ax1[1].get_ylim())
    ax1[1].set_xscale('log')
    ax1[1].set_yscale('log')
    ax2.set_yscale('log')

    # Color the spines and tick labels
    # ax1[1].spines['left'].set_color('blue')
    # ax1[1].tick_params(axis='y', colors='blue')
    # ax2.spines['right'].set_color('red')
    # ax2.tick_params(axis='y', colors='red')

    # Set the same linestyle and make the spine thicker for visibility
    ax1[1].spines['left'].set_linestyle(line_styles['create_times'])
    ax1[1].spines['left'].set_linewidth(2.5)  # Increase the line width for visibility
    ax2.spines['right'].set_linestyle(line_styles['read_times'])
    ax2.spines['right'].set_linewidth(2.5)  # Increase the line width for visibility

    ax1[1].tick_params(axis='both', which='major', length=10, width=2, direction='out')
    ax2.tick_params(axis='both', which='major', length=10, width=2, direction='out')
    
    # Hide the right spine on ax1[1] and left spine on ax2 to prevent overlap
    ax1[1].spines['right'].set_visible(False)
    ax2.spines['left'].set_visible(False)

    # Combine legends from both axes
    lines_1, labels_1 = ax1[1].get_legend_handles_labels()
    lines_2, labels_2 = ax2.get_legend_handles_labels()
    ax1[1].legend(lines_1 + lines_2, labels_1 + labels_2, loc='upper left')
    
    ax1[1].grid(True)

    plt.tight_layout()
    plt.show()



    
def color_diff_log_inset_plot(savefig=None):
    # Initialize a figure and axes
    fig, ax1 = plt.subplots(figsize=(10, 6))


    # Define a color map for different databases
    # colors = {
    #     'sqlite': "#3B9AB2",
    #     'mongodb': "#EBCC2A",
    #     'parquetdb': "#F21A00"
    # }
    colors = {
        'sqlite': "#FF0000",
        'mongodb': "#F2AD00",
        'parquetdb': "#5BBCD6"
    }
    
    
    line_styles = {
        'create_times': 'solid',
        'read_times': 'dashed',
    }

    # Loop through each database benchmark data and plot
    for benchmark_dir in benchmark_dirs:
        basename = os.path.basename(benchmark_dir)
        csv_filename = os.path.join(benchmark_dir, f'{basename}_benchmark.csv')
        
        df = pd.read_csv(csv_filename)

        # Plot create times on the primary y-axis
        ax1.plot(df['n_rows'], df['create_times'], label=f'{basename} create', color=colors[basename], linestyle=line_styles['create_times'])

    # Create twin axis for the secondary y-axis
    ax2 = ax1.twinx()

    for benchmark_dir in benchmark_dirs:
        basename = os.path.basename(benchmark_dir)
        csv_filename = os.path.join(benchmark_dir, f'{basename}_benchmark.csv')
        
        df = pd.read_csv(csv_filename)

        # Plot read times on the secondary y-axis
        ax2.plot(df['n_rows'], df['read_times'], label=f'{basename} read', color=colors[basename], linestyle=line_styles['read_times'])

    # Set labels and title
    ax1.set_xlabel('Number of Rows')
    ax1.set_ylabel('Create Times (s)')
    ax2.set_ylabel('Read Times (s)')
    
    # Ensure both axes have a common scale by linking their limits
    # ax2.set_ylim(ax1.get_ylim())
    # ax1.set_xscale('log')
    # ax1.set_yscale('log')
    # ax2.set_yscale('log')

    # Color the spines and tick labels
    # ax1.spines['left'].set_color('blue')
    # ax1.tick_params(axis='y', colors='blue')
    # ax2.spines['right'].set_color('red')
    # ax2.tick_params(axis='y', colors='red')

    # Set the same linestyle and make the spine thicker for visibility
    ax1.spines['left'].set_linestyle(line_styles['create_times'])
    ax1.spines['left'].set_linewidth(2.5)  # Increase the line width for visibility
    ax2.spines['right'].set_linestyle(line_styles['read_times'])
    ax2.spines['right'].set_linewidth(2.5)  # Increase the line width for visibility

    # Hide the right spine on ax1 and left spine on ax2 to prevent overlap
    ax1.spines['right'].set_visible(False)
    ax2.spines['left'].set_visible(False)

    ax1.tick_params(axis='both', which='major', length=10, width=2, direction='out')
    # ax1.tick_params(axis='both', which='minor', length=100, width=2, direction='out')
    ax2.tick_params(axis='both', which='major', length=10, width=2, direction='out')
    # ax2.tick_params(axis='both', which='minor', length=10, width=2, direction='out')

    
    ax1.grid(True)
    
    scale=36
    ax_inset = inset_axes(ax1, width=f"{scale}%", height=f"{scale}%", loc="upper left", bbox_to_anchor=(0.05, -0.03,1,1), bbox_transform=ax1.transAxes, borderpad=2)

    # Add an inset with log scale
    # ax_inset = inset_axes(ax1, width="30%", height="30%", loc="upper left", bbox_to_anchor=(0,0,1,1))
    # ax_inset = inset_axes(ax1, width="30%", height="30%", bbox_to_anchor=(0,0,1,1))
    ax_inset.grid(True)
    # Plot the same data with log scales
    for benchmark_dir in benchmark_dirs:
        basename = os.path.basename(benchmark_dir)
        csv_filename = os.path.join(benchmark_dir, f'{basename}_benchmark.csv')
        
        df = pd.read_csv(csv_filename)
        
        ax_inset.plot(df['n_rows'], df['create_times'], label=f'{basename} create', color=colors[basename], linestyle=line_styles['create_times'],  linewidth=2)
        # ax_inset.plot(df['n_rows'], df['read_times'], label=f'{basename} read', color=colors[basename], linestyle='--')

    
    
    # Create a twin axis for the secondary y-axis (inset)
    ax_inset2 = ax_inset.twinx()

    # Plot the read_times in the inset (secondary y-axis)
    for benchmark_dir in benchmark_dirs:
        basename = os.path.basename(benchmark_dir)
        csv_filename = os.path.join(benchmark_dir, f'{basename}_benchmark.csv')
        
        df = pd.read_csv(csv_filename)

        ax_inset2.plot(df['n_rows'], df['read_times'], label=f'{basename} read', color=colors[basename], linestyle=line_styles['read_times'], linewidth=2)

    # Set log scale for both axes in the inset
    ax_inset.set_xscale('log')
    ax_inset.set_yscale('log')
    ax_inset2.set_yscale('log')
    
    # Set labels for inset plot
    ax_inset.set_xlabel('Number of Rows (log)', fontsize=8)
    ax_inset.set_ylabel('Create Time (log)', fontsize=8, labelpad=-2)
    ax_inset2.set_ylabel('Read Time (log)', fontsize=8)
    
    nticks = 9
    maj_loc = ticker.LogLocator(numticks=nticks)
    min_loc = ticker.LogLocator(subs='all', numticks=nticks)
    ax_inset.xaxis.set_major_locator(maj_loc)
    ax_inset.xaxis.set_minor_locator(min_loc)
    
    # Set the same linestyle and make the spine thicker for visibility
    ax_inset.spines['left'].set_linestyle(line_styles['create_times'])
    ax_inset.spines['left'].set_linewidth(2.5)  # Increase the line width for visibility
    ax_inset2.spines['right'].set_linestyle(line_styles['read_times'])
    ax_inset2.spines['right'].set_linewidth(2.5)  # Increase the line width for visibility

    # Hide the right spine on ax1 and left spine on ax2 to prevent overlap
    ax_inset.spines['right'].set_visible(False)
    ax_inset2.spines['left'].set_visible(False)
    
    # ax_inset.tick_params(axis='y', which='major', labelsize=8, length=6, width=1.5, direction='out')
    # ax_inset.tick_params(axis='both', which='minor', direction='out')
    # ax_inset2.tick_params(axis='y', which='major', labelsize=8, length=6, width=1.5, direction='out')
    # ax_inset2.tick_params(axis='both', which='minor', labelsize=8, width=1.5, direction='out')
    ax_inset.tick_params(axis='both', which='major', length=6, width=1.5, direction='out')
    ax_inset.tick_params(axis='x', which='minor', length=3, width=1, direction='out')
    ax_inset.tick_params(axis='y', which='minor', length=3, width=1, direction='out')
    ax_inset2.tick_params(axis='y', which='minor', length=3, width=0, direction='out')

    ax_inset2.tick_params(axis='both', which='major', length=6, width=1.5, direction='out')
    
    # ax_inset.tick_params(axis='x', which='minor', length=6, direction='out')  # Hide minor ticks for the left axis
    # ax_inset2.tick_params(axis='both', which='minor', length=6, direction='out')
    # ax_inset2.tick_params(axis='both', which='minor', length=3, width=1, direction='out')

    # You can also force the ticks to be visible by manually setting locators
    # from matplotlib.ticker import LogLocator
    # ax_inset.yaxis.set_major_locator(LogLocator(base=10.0))
    # ax_inset2.yaxis.set_major_locator(LogLocator(base=10.0))

    # # Optionally, set the minor ticks as well if needed
    # ax_inset.yaxis.set_minor_locator(LogLocator(base=10.0, subs='auto', numticks=10))
    # ax_inset2.yaxis.set_minor_locator(LogLocator(base=10.0, subs='auto', numticks=10))
    
    # ax_inset.set_title('Log Plot', fontsize=10)


    
    
    
    
        
    # Combine legends from both axes
    lines_1, labels_1 = ax1.get_legend_handles_labels()
    lines_2, labels_2 = ax2.get_legend_handles_labels()
    ax1.legend(lines_1 + lines_2, labels_1 + labels_2, loc='upper center',  bbox_to_anchor=(0.15, 0,1,1))
    
    ax1.set_title('Benchmark for Create and Read: \n SQLite, MongoDB, and ParquetDB with 100 integer columns')
    plt.tight_layout()
    
    

    if savefig:
        plt.savefig(savefig)
        return None
    plt.show()
    
    
    

    
    
if __name__ == '__main__':
    from parquetdb.utils import matplotlib_utils
    
    matplotlib_utils.set_palette('Zissou1')
    # color_diff_plot()
    # line_style_diff()
    # color_diff_log_two_axes_plot()
    
    # color_diff_log_inset_plot()
    color_diff_log_inset_plot(savefig=os.path.join(banchmark_dir, 'benchmark_create_read_times.pdf'))
    color_diff_log_inset_plot(savefig=os.path.join(banchmark_dir, 'benchmark_create_read_times.png'))
    