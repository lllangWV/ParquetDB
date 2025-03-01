import itertools
import os
import random
import shutil
import string
import sys
import time

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.fs as fs
import pyarrow.parquet as pq
from mpl_toolkits.axes_grid1.inset_locator import inset_axes

from parquetdb import ParquetDB, config

plt.rcParams["axes.labelsize"] = 18
plt.rcParams["axes.titlesize"] = 18
plt.rcParams["xtick.labelsize"] = 14
plt.rcParams["ytick.labelsize"] = 14


db_names = ["sqlite", "mongodb", "parquetdb"]
benchmark_dir = os.path.join(config.data_dir, "benchmarks")
benchmark_dirs = [os.path.join(benchmark_dir, db_name) for db_name in db_names]

benchmark_type = "update"


benchmark_files_dict = {
    "sqlite": ["sqlite_update_into_constant_rows_benchmark.csv"],
    "mongodb": ["mongodb_update_into_constant_rows_benchmark.csv"],
    "parquetdb": ["parquetdb_update_into_constant_rows_benchmark.csv"],
}

line_styles = {"sqlite": "solid", "mongodb": "solid", "parquetdb": "solid"}

marker_styles = {
    "sqlite": "o",
    "mongodb": "o",
    "parquetdb": "o",
}


marker_fill_styles = {
    "sqlite": "full",
    "mongodb": "full",
    "parquetdb": "full",
}


def color_diff_log_inset_plot(savefig=None):
    # Initialize a figure and axes
    fig, ax1 = plt.subplots(figsize=(10, 6))

    # Define a color map for different databases
    # colors = {
    #     'sqlite': "#3B9AB2",
    #     'mongodb': "#EBCC2A",
    #     'parquetdb': "#F21A00"
    # }
    colors = {"sqlite": "#FF0000", "mongodb": "#F2AD00", "parquetdb": "#5BBCD6"}

    # Loop through each database benchmark data and plot
    for db_name, benchmark_files in benchmark_files_dict.items():
        for benchmark_file in benchmark_files:
            basename = os.path.basename(benchmark_file)
            csv_filename = os.path.join(benchmark_dir, db_name, benchmark_file)

            df = pd.read_csv(csv_filename)

            if "with_index" in benchmark_file:
                line_style = line_styles[db_name]["with_index"]
                label = f"{db_name} with index"
            elif "without_index" in benchmark_file:
                line_style = line_styles[db_name]["without_index"]
                label = f"{db_name} without index"
            else:
                line_style = line_styles[db_name]
                label = f"{db_name}"

            # Plot create times on the primary y-axis
            ax1.plot(
                df["n_rows"],
                df["update_times"],
                label=label,
                color=colors[db_name],
                linestyle=line_style,
                marker=marker_styles[db_name],
                fillstyle=marker_fill_styles[db_name],
            )

    # Create twin axis for the secondary y-axis

    # Set labels and title
    ax1.set_xlabel("Number of Rows")
    ax1.set_ylabel("Update Times (s)")
    # ax2.set_ylabel('Read Times (s)')

    # Ensure both axes have a common scale by linking their limits
    # ax1.set_xscale('log')
    # ax1.set_yscale('log')

    # Color the spines and tick labels
    # ax1.spines['left'].set_color('blue')
    # ax1.tick_params(axis='y', colors='blue')

    # Set the same linestyle and make the spine thicker for visibility
    ax1.spines["left"].set_linestyle("solid")
    ax1.spines["left"].set_linewidth(2.5)  # Increase the line width for visibility

    # Hide the right spine on ax1 and left spine on ax2 to prevent overlap
    # ax1.spines['right'].set_visible(False)

    ax1.tick_params(axis="both", which="major", length=10, width=2, direction="out")

    ax1.grid(True)

    scale = 36
    ax_inset = inset_axes(
        ax1,
        width=f"{scale}%",
        height=f"{scale}%",
        loc="upper left",
        bbox_to_anchor=(0.05, -0.03, 1, 1),
        bbox_transform=ax1.transAxes,
        borderpad=2,
    )

    # Add an inset with log scale
    # ax_inset = inset_axes(ax1, width="30%", height="30%", loc="upper left", bbox_to_anchor=(0,0,1,1))
    # ax_inset = inset_axes(ax1, width="30%", height="30%", bbox_to_anchor=(0,0,1,1))
    ax_inset.grid(True)
    # Plot the same data with log scales
    for db_name, benchmark_files in benchmark_files_dict.items():
        for benchmark_file in benchmark_files:
            basename = os.path.basename(benchmark_file)
            csv_filename = os.path.join(benchmark_dir, db_name, benchmark_file)

            df = pd.read_csv(csv_filename)

            if "with_index" in benchmark_file:
                line_style = line_styles[db_name]["with_index"]
                label = f"{db_name} with index"
            elif "without_index" in benchmark_file:
                line_style = line_styles[db_name]["without_index"]
                label = f"{db_name} without index"
            else:
                line_style = line_styles[db_name]
                label = f"{db_name}"

            ax_inset.plot(
                df["n_rows"],
                df["update_times"],
                label=label,
                color=colors[db_name],
                linestyle=line_style,
                marker=marker_styles[db_name],
                fillstyle=marker_fill_styles[db_name],
                linewidth=2,
            )

    # Set log scale for both axes in the inset
    ax_inset.set_xscale("log")
    ax_inset.set_yscale("log")

    # Set labels for inset plot
    ax_inset.set_xlabel("Number of Rows (log)", fontsize=8)
    ax_inset.set_ylabel("Update Time (log)", fontsize=8, labelpad=-2)
    # ax_inset2.set_ylabel('Read Time (log)', fontsize=8)

    nticks = 9
    maj_loc = ticker.LogLocator(numticks=nticks)
    min_loc = ticker.LogLocator(subs="all", numticks=nticks)
    ax_inset.xaxis.set_major_locator(maj_loc)
    ax_inset.xaxis.set_minor_locator(min_loc)

    # Set the same linestyle and make the spine thicker for visibility
    ax_inset.spines["left"].set_linestyle("solid")
    ax_inset.spines["left"].set_linewidth(2.5)  # Increase the line width for visibility

    # Hide the right spine on ax1 and left spine on ax2 to prevent overlap
    # ax_inset.spines['right'].set_visible(False)

    ax_inset.tick_params(
        axis="both", which="major", length=6, width=1.5, direction="out"
    )
    ax_inset.tick_params(axis="x", which="minor", length=3, width=1, direction="out")
    ax_inset.tick_params(axis="y", which="minor", length=3, width=1, direction="out")

    lines_1, labels_1 = ax1.get_legend_handles_labels()

    ax1.legend(lines_1, labels_1, loc="upper center", bbox_to_anchor=(0.12, 0, 1, 1))

    ax1.set_title(
        "Update Benchmark: \n SQLite, MongoDB, and ParquetDB with 100 integer columns"
    )
    plt.tight_layout()

    if savefig:
        plt.savefig(savefig)
        return None
    plt.show()


if __name__ == "__main__":
    from parquetdb.utils import matplotlib_utils

    matplotlib_utils.set_palette("Zissou1")

    # color_diff_log_inset_plot()
    color_diff_log_inset_plot(
        savefig=os.path.join(
            benchmark_dir, "benchmark_update_into_constant_rows_times.pdf"
        )
    )
    color_diff_log_inset_plot(
        savefig=os.path.join(
            benchmark_dir, "benchmark_update_into_constant_rows_times.png"
        )
    )
