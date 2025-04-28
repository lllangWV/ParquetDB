import os
from typing import List

import numpy as np
import pyarrow as pa

from parquetdb.core.parquetdb import ParquetDB


def join_tables(
    left_table,
    right_table,
    left_keys: List[str],
    right_keys: List[str] = None,
    join_type: str = "left outer",
    left_suffix: str = None,
    right_suffix: str = None,
    coalesce_keys: bool = True,
):
    """
    Join two PyArrow tables based on specified key columns.

    Parameters
    ----------
    left_table : pyarrow.Table
        The left table for the join operation.
    right_table : pyarrow.Table
        The right table for the join operation.
    left_keys : str or list of str
        Column name(s) from the left table to join on.
    right_keys : str or list of str, optional
        Column name(s) from the right table to join on. If None, uses left_keys.
    join_type : str, optional
        Type of join to perform. Default is "left outer".
        Supported types: "left outer", "right outer", "inner", "full outer".
    left_suffix : str, optional
        Suffix to append to overlapping column names from the left table.
    right_suffix : str, optional
        Suffix to append to overlapping column names from the right table.
    coalesce_keys : bool, optional
        Whether to combine join keys that appear in both tables. Default is True.

    Returns
    -------
    pyarrow.Table
        A new table containing the joined data with:
        - All columns from both tables (except join keys which are coalesced if specified)
        - Column name conflicts resolved using the provided suffixes
        - Combined metadata from both input tables

    Notes
    -----
    The function:
    - Preserves the original data types and metadata
    - Handles overlapping column names by adding suffixes
    - Removes temporary index columns used for joining
    - Combines metadata from both input tables

    Examples
    --------
    >>> left = pa.table({'id': [1, 2, 3], 'value': ['a', 'b', 'c']})
    >>> right = pa.table({'id': [2, 3, 4], 'score': [10, 20, 30]})
    >>> joined = join_tables(left, right, 'id')
    >>> joined.column_names
    ['id', 'value', 'score']
    """
    log = ""
    # Add index column to both tables
    if right_keys:
        tmp_right_table = right_table.select(right_keys)
        log += f"\n tmp_right_table: \n {tmp_right_table} \n"
        tmp_right_table = tmp_right_table.append_column(
            "right_index", pa.array(np.arange(len(tmp_right_table)))
        )
    else:
        tmp_right_table = pa.Table.from_arrays(
            [pa.array(np.arange(len(right_table)))], names=["right_index"]
        )

    log += "=" * 100

    log += f"\n tmp_right_table: \n {tmp_right_table} \n"

    log += "=" * 100
    tmp_left_table = left_table.select(left_keys)

    log += f"\n tmp_left_table: \n {tmp_left_table} \n"

    log += "=" * 100
    tmp_left_table = tmp_left_table.append_column(
        "left_index", pa.array(np.arange(len(tmp_left_table)))
    )

    log += f"\n tmp_left_table: \n {tmp_left_table} \n"

    log += "=" * 100
    joined_table = tmp_left_table.join(
        tmp_right_table,
        keys=left_keys,
        right_keys=right_keys,
        join_type=join_type,
        coalesce_keys=coalesce_keys,
    )

    log += f"\n joined_table: \n {joined_table} \n"

    log += "=" * 100
    right_columns = set(right_table.column_names)
    left_columns = set(left_table.column_names)

    log += f"\n right_columns: \n {right_columns} \n"
    log += f"\n left_columns: \n {left_columns} \n"

    log += "=" * 100

    metadata = {}

    try:
        # left_table = left_table.sort_by()
        for column_name in left_columns:
            field = left_table.schema.field(column_name)
            if column_name in left_keys:
                continue
            new_column = left_table[column_name].take(joined_table["left_index"])
            log += f"\n new_column: \n {new_column} \n"
            if (
                left_suffix
                and column_name in right_columns
                and column_name in left_columns
                and "right_index" in joined_table.column_names
            ):
                column_name = column_name + left_suffix

            new_field = field.with_name(column_name)
            joined_table = joined_table.add_column(0, new_field, new_column)
            log += f"\n joined_table after left: \n {joined_table} \n"

        left_metadata = left_table.schema.metadata
        if left_metadata:
            metadata.update(left_metadata)
    except Exception as e:
        log += f"Error: \n {e} \n"
        pass

    try:

        for column_name in right_columns:
            field = right_table.schema.field(column_name)
            if column_name in right_keys:
                continue
            new_column = right_table[column_name].take(joined_table["right_index"])
            if (
                right_suffix
                and column_name in right_columns
                and column_name in left_columns
                and "left_index" in joined_table.column_names
            ):
                column_name = column_name + right_suffix

            new_field = field.with_name(column_name)
            joined_table = joined_table.append_column(new_field, new_column)
            log += f"\n joined_table after right: \n {joined_table} \n"

        right_metadata = right_table.schema.metadata
        if right_metadata:
            metadata.update(right_metadata)
    except Exception as e:
        log += f"\n Error: \n {e} \n"
        pass

    # joined_table = joined_table.sort_by(
    #     [("left_index", "ascending"), ("right_index", "ascending")]
    # )
    if "right_index" in joined_table.column_names:
        joined_table = joined_table.drop(["right_index"])
    if "left_index" in joined_table.column_names:
        joined_table = joined_table.drop(["left_index"])

    joined_table = joined_table.replace_schema_metadata(metadata)
    log += f"\n joined_table: \n {joined_table} \n"
    log += "-" * 200
    return joined_table, log


def left_outer_join():
    current_data = [
        {"id_1": 100, "id_2": 10, "field_1": "here"},
        {"id_1": 33, "id_2": 12},
        {"id_1": 12, "id_2": 13, "field_2": "field_2"},
    ]

    current_table = ParquetDB.construct_table(current_data)

    # Create second table with salary data
    incoming_data = [
        {"id_1": 100, "id_2": 10, "field_2": "there"},
        {"id_1": 5, "id_2": 5},
        {"id_1": 33, "id_2": 13},  # Note: emp_id 4 doesn't exist in employees
        {
            "id_1": 33,
            "id_2": 12,
            "field_2": "field_2",
            "field_3": "field_3",
        },  # Note: emp_id 4 doesn't exist in employees
    ]

    incoming_table = ParquetDB.construct_table(incoming_data)

    join_type = "right outer"
    left_outer_table_pyarrow = incoming_table.join(
        current_table,
        keys=["id_1", "id_2"],
        right_keys=["id_1", "id_2"],
        left_suffix="_incoming",
        right_suffix="_current",
        join_type=join_type,
    )

    column_names = left_outer_table_pyarrow.column_names
    names_sorted = sorted(column_names)
    left_outer_table_pyarrow_sorted = left_outer_table_pyarrow.select(names_sorted)

    left_outer_table, log = join_tables(
        incoming_table,
        current_table,
        left_keys=["id_1", "id_2"],
        right_keys=["id_1", "id_2"],
        left_suffix="_incoming",
        right_suffix="_current",
        join_type=join_type,
    )

    column_names = left_outer_table.column_names
    names_sorted = sorted(column_names)
    left_outer_table_sorted = left_outer_table.select(names_sorted)
    # left_outer_table_pyarrow_sorted = left_outer_table_pyarrow_sorted.sort_by(
    #     [("id_1", "ascending"), ("id_2", "ascending")]
    # )
    # left_outer_table_sorted = left_outer_table_sorted.sort_by(
    #     [("id_1", "ascending"), ("id_2", "ascending")]
    # )

    log += f"\n pyarrow_join: \n {left_outer_table_pyarrow} \n"
    log += f"\n custom_join: \n {left_outer_table} \n"
    for name in left_outer_table_sorted.column_names:

        pyarrow_sort = left_outer_table_pyarrow_sorted[name].to_pylist()
        custom_sort = left_outer_table_sorted[name].to_pylist()
        log += f"\n custom_sort: \n {custom_sort} \n"
        log += f"\n pyarrow_sort: \n {pyarrow_sort} \n"
        try:
            assert custom_sort == pyarrow_sort
        except Exception as e:
            log += f"\n Error in assertion: \n {e} \n"
            return log, True

    return log, False


root_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(root_dir)
log_dir = os.path.join(parent_dir, "logs")
pytest_failure_logs_dir = os.path.join(log_dir, "pytest_failure_logs")
os.makedirs(pytest_failure_logs_dir, exist_ok=True)

count = 0
written_correct_log = False
while True:
    count += 1
    count_str = str(count)
    log, has_error = left_outer_join()

    if has_error:

        with open(
            os.path.join(pytest_failure_logs_dir, "right_outer_join.log"), "w"
        ) as f:
            log = f"Iteration {count_str}: \n {log}"
            f.write(log)
        break

    elif not written_correct_log:
        written_correct_log = True
        with open(
            os.path.join(pytest_failure_logs_dir, "correct_right_outer_join.log"), "w"
        ) as f:
            log = f"Iteration {count_str}: \n {log}"
            f.write(log)
