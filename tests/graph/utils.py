import os
import logging
import warnings
from pathlib import Path

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from parquetdb import ParquetDB
from parquetdb.graph import edge_generator
from parquetdb.graph import node_generator
from parquetdb.utils import pyarrow_utils

logger = logging.getLogger(__name__)


GRAPH_TEST_DIR = Path(__file__).parent
DATA_DIR = GRAPH_TEST_DIR / "data"

BASE_ELEMENT_FILE = DATA_DIR / "interim_periodic_table_values.parquet"


@node_generator
def element(base_filepath=BASE_ELEMENT_FILE):
    """
    Creates Element nodes if no file exists, otherwise loads them from a file.
    """

    logger.info(f"Initializing element nodes from {base_filepath}")
    # Suppress warnings during node creation
    warnings.filterwarnings("ignore", category=UserWarning)

    try:
        file_ext = os.path.splitext(base_filepath)[-1][1:]
        logger.debug(f"File extension: {file_ext}")
        if file_ext == "parquet":
            df = pd.read_parquet(base_filepath)
        elif file_ext == "csv":
            df = pd.read_csv(base_filepath, index_col=0)
        else:
            raise ValueError(f"base_filepath must be a parquet or csv file")
        logger.debug(f"Read element dataframe shape {df.shape}")

        df["oxidation_states"] = df["oxidation_states"].apply(
            lambda x: x.replace("]", "").replace("[", "")
        )
        df["oxidation_states"] = df["oxidation_states"].apply(
            lambda x: ",".join(x.split())
        )
        df["oxidation_states"] = df["oxidation_states"].apply(
            lambda x: eval("[" + x + "]")
        )
        df["experimental_oxidation_states"] = df["experimental_oxidation_states"].apply(
            lambda x: eval(x)
        )
        df["ionization_energies"] = df["ionization_energies"].apply(lambda x: eval(x))

    except Exception as e:
        logger.error(f"Error reading element CSV file: {e}")
        return None

    return df


@edge_generator
def element_element_neighborsByGroupPeriod(element_store):

    try:
        connection_name = "neighborsByGroupPeriod"
        table = element_store.read_nodes(
            columns=["atomic_number", "extended_group", "period", "symbol"]
        )
        element_df = table.to_pandas(split_blocks=True, self_destruct=True)

        # Getting group-period edge index
        edge_index = get_group_period_edge_index(element_df)

        # Creating the relationships dataframe
        df = pd.DataFrame(edge_index, columns=[f"source_id", f"target_id"])

        # Dropping rows with NaN values and casting to int64
        df = df.dropna().astype(np.int64)

        # Add source and target type columns
        df["source_type"] = element_store.node_type
        df["target_type"] = element_store.node_type
        df["edge_type"] = connection_name
        df["weight"] = 1.0

        table = ParquetDB.construct_table(df)

        reduced_table = element_store.read(
            columns=["symbol", "id", "extended_group", "period"]
        )
        reduced_source_table = reduced_table.rename_columns(
            {
                "symbol": "source_name",
                "extended_group": "source_extended_group",
                "period": "source_period",
            }
        )
        reduced_target_table = reduced_table.rename_columns(
            {
                "symbol": "target_name",
                "extended_group": "target_extended_group",
                "period": "target_period",
            }
        )

        table = pyarrow_utils.join_tables(
            table,
            reduced_source_table,
            left_keys=["source_id"],
            right_keys=["id"],
            join_type="left outer",
        )

        table = pyarrow_utils.join_tables(
            table,
            reduced_target_table,
            left_keys=["target_id"],
            right_keys=["id"],
            join_type="left outer",
        )

        names = pc.binary_join_element_wise(
            pc.cast(table["source_name"], pa.string()),
            pc.cast(table["target_name"], pa.string()),
            f"_{connection_name}_",
        )

        table = table.append_column("name", names)

        logger.debug(
            f"Created element-group-period relationships. Shape: {table.shape}"
        )
    except Exception as e:
        logger.exception(f"Error creating element-group-period relationships: {e}")
        raise e

    return table


S_COLUMNS = np.arange(1, 3)
P_COLUMNS = np.arange(27, 33)
D_COLUMNS = np.arange(17, 27)
F_COLUMNS = np.arange(3, 17)


def get_group_period_edge_index(df):
    """
    Generate a list of edge indexes based on atomic numbers, extended groups, and periods from the provided dataframe.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing columns 'atomic_number', 'extended_group', 'period', and 'symbol'.

    Returns
    -------
    list
        A list of tuples representing edge indexes between atomic elements.

    Example
    -------
    >>> df = pd.DataFrame({'atomic_number': [1, 2], 'extended_group': [1, 18], 'period': [1, 1], 'symbol': ['H', 'He']})
    >>> get_group_period_edge_index(df)
    [(0, 1)]
    """
    if "atomic_number" not in df.columns:
        raise ValueError("Dataframe must contain 'atomic_number' column")
    if "extended_group" not in df.columns:
        raise ValueError("Dataframe must contain 'extended_group' column")
    if "period" not in df.columns:
        raise ValueError("Dataframe must contain 'period' column")
    if "symbol" not in df.columns:
        raise ValueError("Dataframe must contain 'symbol' column")

    edge_index = []
    for irow, row in df.iterrows():
        symbol = row["symbol"]
        atomic_number = row["atomic_number"]
        extended_group = row["extended_group"]
        period = row["period"]

        if extended_group in S_COLUMNS:
            # Hydrogen
            if period == 1:
                period_neighbors = (None, 1)
                atomic_number_neighbors = (None, None)

            # Lithium
            elif extended_group == 1 and period == 2:
                period_neighbors = (-1, 1)
                atomic_number_neighbors = (None, 1)
            # Francium
            elif extended_group == 1 and period == 7:
                period_neighbors = (-1, None)
                atomic_number_neighbors = (None, 1)
            # Beryllium
            elif extended_group == 2 and period == 2:
                period_neighbors = (None, 1)
                atomic_number_neighbors = (-1, 1)
            # Radium
            elif extended_group == 2 and period == 7:
                period_neighbors = (-1, None)
                atomic_number_neighbors = (-1, 1)
            elif extended_group == 1:
                period_neighbors = (-1, 1)
                atomic_number_neighbors = (None, 1)
            else:
                period_neighbors = (-1, 1)
                atomic_number_neighbors = (-1, 1)

        if extended_group in P_COLUMNS:
            # Helium
            if period == 1:
                period_neighbors = (None, 1)
                atomic_number_neighbors = (None, None)
            # Boron
            elif extended_group == 27 and period == 2:
                period_neighbors = (None, 1)
                atomic_number_neighbors = (-1, 1)
            # Nihonium
            elif extended_group == 27 and period == 7:
                period_neighbors = (-1, None)
                atomic_number_neighbors = (-1, 1)
            # Neon
            elif extended_group == 32 and period == 2:
                period_neighbors = (None, 1)
                atomic_number_neighbors = (-1, None)
            # Oganesson
            elif extended_group == 32 and period == 7:
                period_neighbors = (-1, None)
                atomic_number_neighbors = (-1, None)
            elif extended_group == 32:
                period_neighbors = (-1, 1)
                atomic_number_neighbors = (-1, None)
            else:
                period_neighbors = (-1, 1)
                atomic_number_neighbors = (-1, 1)

        if extended_group in D_COLUMNS:
            # Scandium
            if extended_group == 17 and period == 4:
                period_neighbors = (None, 1)
                atomic_number_neighbors = (-1, 1)
            # Lawrencium
            elif extended_group == 17 and period == 7:
                period_neighbors = (-1, None)
                atomic_number_neighbors = (-1, 1)
            # Zinc
            elif extended_group == 26 and period == 4:
                period_neighbors = (None, 1)
                atomic_number_neighbors = (-1, 1)
            # Copernicium
            elif extended_group == 26 and period == 7:
                period_neighbors = (-1, None)
                atomic_number_neighbors = (-1, 1)
            else:
                period_neighbors = (-1, 1)
                atomic_number_neighbors = (-1, 1)

        if extended_group in F_COLUMNS:
            # Lanthanum
            if extended_group == 3 and period == 6:
                period_neighbors = (None, 1)
                atomic_number_neighbors = (-1, 1)
            # Actinium
            elif extended_group == 3 and period == 7:
                period_neighbors = (-1, None)
                atomic_number_neighbors = (-1, 1)
            # Zinc
            elif extended_group == 16 and period == 6:
                period_neighbors = (None, 1)
                atomic_number_neighbors = (-1, 1)
            # Copernicium
            elif extended_group == 16 and period == 7:
                period_neighbors = (-1, None)
                atomic_number_neighbors = (-1, 1)
            else:
                period_neighbors = (-1, 1)
                atomic_number_neighbors = (-1, 1)

        for neighbor_period in period_neighbors:
            current_period = period

            if neighbor_period is not None:
                current_period += neighbor_period

                matching_indexes = df[
                    (df["period"] == current_period)
                    & (df["extended_group"] == extended_group)
                ].index.values
                if len(matching_indexes) != 0:

                    edge_index.append((irow, matching_indexes[0]))

        for neighbor_atomic_number in atomic_number_neighbors:
            current_atomic_number = atomic_number

            if neighbor_atomic_number is not None:
                current_atomic_number += neighbor_atomic_number
                matching_indexes = df[
                    df["atomic_number"] == current_atomic_number
                ].index.values
                if len(matching_indexes) != 0:
                    edge_index.append((irow, matching_indexes[0]))
    return edge_index
