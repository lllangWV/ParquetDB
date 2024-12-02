import json
import os

from matplotlib import rcParams
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from mpl_toolkits.axes_grid1.inset_locator import inset_axes

import pyarrow as pa

from parquetdb import ParquetDB, config


from parquetdb.utils import matplotlib_utils
import matplotlib.ticker as ticker

def main():
    db_names=['sqlite','mongodb','parquetdb','alexandria']
    banchmark_dir=os.path.join(config.data_dir, 'benchmarks')
    benchmark_dirs=[os.path.join(banchmark_dir, db_name) for db_name in db_names]


    with open(os.path.join(banchmark_dir, 'alexandria', 'create_load_benchmark_alexandria_3D.json'), 'r') as f:
        create_load_data=json.load(f)
        
    with open(os.path.join(banchmark_dir, 'alexandria', 'task_benchmark_alexandria_3D.json'), 'r') as f:
        task_data=json.load(f)
        
        
    # plot_load_create_times(data['create_times'],data['json_load_times'],data['n_rows_per_file'])
    # plot_load_create_times(create_load_data['create_times'],create_load_data['json_load_times'],create_load_data['n_rows_per_file'],
    #                        savefig=os.path.join(banchmark_dir, 'alexandria_create_benchmark.png'))
    
    # plot_load_create_times(create_load_data['create_times'],create_load_data['json_load_times'],create_load_data['n_rows_per_file'],
    #                        savefig=os.path.join(banchmark_dir, 'alexandria_create_benchmark.pdf'))

    
    # operations_plot(task_data['task_names'],task_data['task_times'])
    
    operations_plot(task_data['task_names'],task_data['task_times'], 
                    savefig=os.path.join(banchmark_dir, 'alexandria_database_operations_benchmark.png'))
    operations_plot(task_data['task_names'],task_data['task_times'],
                    savefig=os.path.join(banchmark_dir, 'alexandria_database_operations_benchmark.pdf'))
    
    
########################################################################################################################

def plot_load_create_times(create_times,json_load_times,n_rows_per_file, savefig=None):
    
    plt.rcParams['axes.labelsize'] = 18
    plt.rcParams['axes.titlesize'] = 18
    plt.rcParams['xtick.labelsize'] = 10
    plt.rcParams['ytick.labelsize'] = 16
    
    
    # Calculate averages and standard deviations
    avg_create_time = np.mean(create_times)
    std_create_time = np.std(create_times)
    avg_json_load_time = np.mean(json_load_times)
    std_json_load_time = np.std(json_load_times)
    
    # Calculate the total sum of create times and json load times
    total_create_time = sum(create_times)
    total_json_load_time = sum(json_load_times)
    total_time = total_create_time + total_json_load_time

    

    # Create the plot
    fig, ax1=plt.subplots(figsize=(12, 8))

    
    # Plot json_load_times
    ax1.bar(range(len(json_load_times)), json_load_times,color="#5BBCD6", label='JSON Load Times', alpha=0.6)

    # Plot create_times
    ax1.bar(range(len(create_times)), create_times, color="#FF0000", label='Create Times', alpha=0.6)

    
    # Display the averages and standard deviations
    ax1.axhline(avg_create_time, color="#FF0000", linestyle='--', label=f'Avg Create Time: {avg_create_time:.2f} s', linewidth=2)
    ax1.axhline(avg_json_load_time, color="#5BBCD6", linestyle='--', label=f'Avg JSON Load Time: {avg_json_load_time:.2f} s', linewidth=2)
    # plt.axhline(avg_create_time + std_create_time, color='blue', linestyle=':', label=f'Std Create Time: {std_create_time:.2f} s')
    # plt.axhline(avg_json_load_time + std_json_load_time, color='red', linestyle=':', label=f'Std JSON Load Time: {std_json_load_time:.2f} s')

    # Transparent bands for standard deviations
    # ax1.fill_between(range(len(create_times)), avg_create_time - std_create_time, avg_create_time + std_create_time, color='blue', alpha=0.2, label=f'Std Create Time: ±{std_create_time:.2f}')
    # ax1.fill_between(range(len(json_load_times)), avg_json_load_time - std_json_load_time, avg_json_load_time + std_json_load_time, color='red', alpha=0.2, label=f'Std JSON Load Time: ±{std_json_load_time:.2f}')

    # Add text box with total time
    total_text = f"Total Create Time: {total_create_time:.2f} s\nTotal JSON Load Time: {total_json_load_time:.2f} s\nTotal Time: {total_time:.2f} s"
    plt.text(0, max(json_load_times) * 0.95, total_text, fontsize=12, bbox=dict(facecolor='white', alpha=0.7), ha='left')

    
    # Add labels and title
    ax1.set_xlabel('File Index. (Number of records per json file)')
    ax1.set_ylabel('Time (seconds)')
    ax1.set_title('Create Times and JSON Load Times with Averages and Standard Deviations')
    
    ax1.set_xticks(range(len(n_rows_per_file)), n_rows_per_file, rotation=90)

    # ax1.set_xticks(range(len(n_rows_per_file)), [f'{x:1.1e}' for x in n_rows_per_file], rotation=90)

    # Plot create_times
    ax1.grid(axis='y', linestyle='solid', alpha=0.3)
    

    # Display the averages and standard deviations
    plt.legend()

    # Show the plot
    plt.tight_layout()
    if savefig:
        plt.savefig(savefig)
        return None
    plt.show()


def operations_plot(task_names,task_times, savefig=None):
    
    # plt.rcParams['axes.labelsize'] = 18
    # plt.rcParams['axes.titlesize'] = 18
    # plt.rcParams['xtick.labelsize'] = 10
    # plt.rcParams['ytick.labelsize'] = 16
    
    xlabel_size=16
    ylabel_size=16
    title_size=16
    xtick_size=14
    ytick_size=12
    
    inset_xtick_size=10
    inset_ytick_size=10
    inset_title_size=12
    
    
    labels=task_names
    times=task_times
    # Create the main plot with numbered x labels and an inset showing the same data on a log scale
    fig, ax = plt.subplots(figsize=(10, 6))

    # Number the labels
    numbered_labels = [f"{i+1}. {label}" for i, label in enumerate(labels)]
    
    # matplotlib_utils.set_palette('Cavalcanti1')
    matplotlib_utils.set_palette('Darjeeling1')
    # matplotlib_utils.set_palette('Zissou1')
    # matplotlib_utils.set_palette('AsteroidCity1')
    # matplotlib_utils.set_palette('BottleRocket2')
    colors = rcParams['axes.prop_cycle'].by_key()['color']
    # Main horizontal bar plotcolors[:len(times)]
    # ax.barh(numbered_labels, times, color="#5BBCD6")
    ax.barh(numbered_labels, times, color=colors[:len(times)])
    ax.set_xlabel('Total Time (seconds)', fontsize=xlabel_size)
    ax.set_ylabel('Operations', fontsize=ylabel_size)
    
    ax.tick_params(axis='x', labelsize=xtick_size)
    ax.tick_params(axis='y', labelsize=ytick_size)
    ax.set_title('Total Time for Various Operations', fontsize=title_size)

    # Inset plot with log scale and just the numbers
    # ax_inset = inset_axes(ax, width="40%", height="30%", loc="center right")
    
    ax_inset = inset_axes(ax, width="30%", height="30%", loc='center right',
                      bbox_to_anchor=(-0.05, -0.1, 1, 1), bbox_transform=ax.transAxes)
    ax_inset.barh(range(1, len(labels)+1), times, color="#FF0000")
    ax_inset.barh(range(1, len(labels)+1), times, color=colors[:len(times)])
    ax_inset.set_xscale('log')
    ax_inset.set_yticks(range(1, len(labels)+1))  # Show just the numbers
    ax_inset.set_yticklabels(range(1, len(labels)+1), fontsize=inset_ytick_size)
    ax_inset.set_title('Log Scale', fontsize=inset_title_size)

    # Adjust layout and show the plot
    plt.tight_layout()
    
    if savefig:
        plt.savefig(savefig)
        return None
    plt.show()

if __name__ == '__main__':
    main()
    
    
    