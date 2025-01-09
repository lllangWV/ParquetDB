import logging
import math
import time
from typing import List, Union

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from parquetdb.utils.general_utils import timeit

logger = logging.getLogger(__name__)

# https://arrow.apache.org/docs/python/api/datatypes.html
t_string = pa.string()
t_int32 = pa.int32()
t_int64 = pa.int64()
t_float32 = pa.float32()
t_float64 = pa.float64()
t_bool = pa.bool_()

# Create variable-length or fixed size binary type.
t_binary = pa.binary()

# one of ‘s’ [second], ‘ms’ [millisecond], ‘us’ [microsecond], or ‘ns’ [nanosecond]
t_timestamp = pa.timestamp("ms")


def find_difference_between_pyarrow_schemas(schema1, schema2):
    """
    Finds the difference between two PyArrow schemas.

    Parameters
    ----------
    schema1 : pyarrow.Schema
        The first schema to compare.
    schema2 : pyarrow.Schema
        The second schema to compare.

    Returns
    -------
    set
        A set of field names that are present in `schema1` but not in `schema2`.

    Examples
    --------
    >>> schema1 = pa.schema([("a", pa.int32()), ("b", pa.string())])
    >>> schema2 = pa.schema([("b", pa.string())])
    >>> find_difference_between_pyarrow_schemas(schema1, schema2)
    {'a'}
    """
    # Create a set of field names from the first schema
    field_names1 = set(schema1)
    # Create a set of field names from the second schema
    field_names2 = set(schema2)
    # Find the difference between the two sets
    difference = field_names1.difference(field_names2)
    return difference


def merge_structs(
    current_type: pa.StructType, incoming_type: pa.StructType
) -> pa.StructType:
    """
    Recursively merges two PyArrow StructTypes.

    Parameters
    ----------
    current_type : pa.StructType
        The existing struct type.
    incoming_type : pa.StructType
        The new struct type to merge with the existing struct.

    Returns
    -------
    pa.StructType
        A new PyArrow StructType representing the merged result of the two input structs.

    Examples
    --------
    >>> current = pa.struct([("a", pa.int32()), ("b", pa.string())])
    >>> incoming = pa.struct([("b", pa.string()), ("c", pa.float64())])
    >>> merge_structs(current, incoming)
    StructType(a: int32, b: string, c: float64)
    """
    # Create a dictionary of the current fields for easy comparison
    current_fields_dict = {field.name: field for field in current_type}
    merged_fields = []

    # Iterate over the incoming fields and check if they exist in the current_type
    for incoming_field in incoming_type:
        if incoming_field.name in current_fields_dict:
            current_field = current_fields_dict[incoming_field.name]

            # If both are structs, recursively merge the fields
            if pa.types.is_struct(incoming_field.type) and pa.types.is_struct(
                current_field.type
            ):
                merged_field = pa.field(
                    incoming_field.name,
                    merge_structs(current_field.type, incoming_field.type),
                )
            else:
                # If the field exists but has a different type, use the incoming field type
                merged_field = incoming_field
        else:
            # If the field does not exist in the current, add the incoming field
            merged_field = incoming_field

        merged_fields.append(merged_field)

    # Add any remaining current fields that were not in the incoming schema
    for current_field in current_type:
        if current_field.name not in {field.name for field in incoming_type}:
            merged_fields.append(current_field)

    return pa.struct(merged_fields)


def merge_schemas(current_schema: pa.Schema, incoming_schema: pa.Schema) -> pa.Schema:
    """
    Merges two PyArrow schemas, combining fields and recursively merging struct fields.

    Parameters
    ----------
    current_schema : pyarrow.Schema
        The existing schema to merge.
    incoming_schema : pyarrow.Schema
        The new schema to merge with the existing schema.

    Returns
    -------
    pa.Schema
        A new PyArrow schema that represents the merged result of the two input schemas.

    Examples
    --------
    >>> current_schema = pa.schema([("a", pa.int32()), ("b", pa.string())])
    >>> incoming_schema = pa.schema([("b", pa.string()), ("c", pa.float64())])
    >>> merge_schemas(current_schema, incoming_schema)
    Schema(a: int32, b: string, c: float64)
    """
    merged_fields = []
    incoming_field_names = {field.name for field in incoming_schema}
    # Iterate through fields in the current schema
    for current_field in current_schema:
        if current_field.name in incoming_field_names:
            incoming_field = incoming_schema.field(current_field.name)

            if pa.types.is_struct(current_field.type) and pa.types.is_struct(
                incoming_field.type
            ):
                # If both fields are structs, merge them recursively
                merged_field = pa.field(
                    current_field.name,
                    merge_structs(current_field.type, incoming_field.type),
                )
            else:
                # Use the incoming field if they are not both structs or if they are identical
                merged_field = incoming_field
        else:
            # If the field does not exist in the incoming schema, keep the current field
            merged_field = current_field

        merged_fields.append(merged_field)

    # Add any fields from the incoming schema that are not in the current
    for incoming_field in incoming_schema:
        if incoming_field.name not in {field.name for field in current_schema}:
            merged_fields.append(incoming_field)

    # Return the merged schema
    return pa.schema(merged_fields)


def replace_empty_structs(
    column_array: pa.Array, dummy_field=pa.field("dummy_field", pa.int16())
):
    """
    Replaces empty PyArrow struct arrays with a struct containing a dummy field.

    Parameters
    ----------
    column_array : pa.Array
        The column array to inspect for empty structs.
    dummy_field : pa.Field, optional
        The dummy field to insert into empty structs. Defaults to a field named 'dummy_field' with type `pa.int16()`.

    Returns
    -------
    pa.Array
        The input array with empty structs replaced by structs containing the dummy field.

    Examples
    --------
    >>> column_array = pa.array([{'a': 1}, {}, {'a': 2}], type=pa.struct([pa.field('a', pa.int32())]))
    >>> replace_empty_structs(column_array)
    <pyarrow.StructArray object at 0x...>
    """
    if isinstance(column_array, pa.ChunkedArray):
        column_array = column_array.combine_chunks()

    if pa.types.is_list(column_array.type):
        arr_data = replace_empty_structs(column_array.values)
        arr_offsets = column_array.offsets
        arr = pa.ListArray.from_arrays(arr_offsets, arr_data)
        return arr

    # Catches non struct field cases
    if not pa.types.is_struct(column_array.type):
        return column_array

    # Catches empty structs cases
    if len(column_array.type) == 0:
        null_array = pa.nulls(len(column_array), dummy_field.type)
        return pc.make_struct(null_array, field_names=[dummy_field.name])

    child_field_names = [field.name for field in column_array.type]
    child_chunked_array_list = column_array.flatten()

    child_arrays = []
    for child_array, child_field_name in zip(
        child_chunked_array_list, child_field_names
    ):
        child_array = replace_empty_structs(child_array)
        child_arrays.append(child_array)

    return pc.make_struct(*child_arrays, field_names=child_field_names)


def replace_empty_structs_in_column(
    table, column_name, dummy_field=pa.field("dummy_field", pa.int16()), is_nested=False
):
    """
    Replaces empty struct values in a specified column of a PyArrow Table with a dummy field.

    This function checks if the given column in the table is of a struct type. If it is, it replaces
    any empty structs in the column with a dummy struct that includes the `dummy_field`. The modified
    column is then updated in the table and returned.

    Parameters
    ----------
    table : pyarrow.Table
        The input PyArrow Table containing the column to be modified.
    column_name : str
        The name of the column in the table where empty structs should be replaced.
    dummy_field : pyarrow.Field, optional
        A dummy field to insert into empty structs, by default `pa.field('dummy_field', pa.int16())`.

    Returns
    -------
    pyarrow.Table
        The updated table where empty structs in the specified column have been replaced with dummy structs.

    Examples
    --------
    >>> import pyarrow as pa
    >>> data = pa.array([{'a': 1}, None, {}], type=pa.struct([('a', pa.int64())]))
    >>> table = pa.table([data], names=['col1'])
    >>> modified_table = replace_empty_structs_in_column(table, 'col1')
    >>> print(modified_table)
    pyarrow.Table
    col1: struct<a: int64, dummy_field: int16>
    ----
    col1: [{a: 1}, {dummy_field: 0}, {dummy_field: 0}]
    """
    column_type = table.schema.field(column_name).type

    has_empty_struct = "struct<>" in str(column_type)

    # Check if any fields in the struct type are empty
    if pa.types.is_struct(column_type) and has_empty_struct:
        logger.debug("This column is a StructArray")

        column_array = table.column(column_name)
        column_index = table.schema.get_field_index(column_name)

        # Replacing empty structs with dummy structs
        if is_nested:
            updated_array = replace_empty_structs(column_array, dummy_field=dummy_field)
        else:
            updated_array = pa.nulls(len(column_array), type=dummy_field.type)

        updated_field = pa.field(f"{column_name}", updated_array.type)
        table = table.set_column(column_index, updated_field, updated_array)
        # updated_field = pa.field(f'{column_name}.dummy', updated_array.type)
        # table = table.set_column(column_index, updated_field, updated_array)

        # column_name=f'{column_name}.dummy'
    elif pa.types.is_list(column_type) and has_empty_struct:
        logger.debug("This column is a ListArray")
        column_array = table.column(column_name)
        column_index = table.schema.get_field_index(column_name)

        arr_data = replace_empty_structs(column_array.combine_chunks().values)
        arr_offsets = column_array.combine_chunks().offsets
        arr = pa.ListArray.from_arrays(arr_offsets, arr_data)
        # new_struct_array = pc.make_struct(arr, field_names=[column_name])
        table = table.set_column(column_index, pa.field(column_name, arr.type), arr)

    return table


def is_empty_struct_in_column(table, column_name):
    column_type = table.schema.field(column_name).type
    if pa.types.is_struct(column_type):
        fields = column_type.fields
        if len(fields) == 0:
            return True
    elif pa.types.is_list(column_type):
        value_type = column_type.value_type
        if pa.types.is_struct(value_type):
            fields = value_type.fields
            if len(fields) == 0:
                return True
    return False


def replace_empty_structs_in_table(
    table, dummy_field=pa.field("dummy_field", pa.int16())
):
    """
    Replaces empty struct fields in a PyArrow table with a struct containing a dummy field.

    Parameters
    ----------
    table : pa.Table
        The table in which to replace empty structs.
    dummy_field : pa.Field, optional
        The dummy field to insert into empty structs. Defaults to a field named 'dummy_field' with type `pa.int16()`.

    Returns
    -------
    pa.Table
        The table with empty struct fields replaced by structs containing the dummy field.

    Examples
    --------
    >>> table = pa.table([{'a': 1}, {}, {'a': 2}], schema=pa.schema([pa.field('a', pa.struct([pa.field('a', pa.int32())]))]))
    >>> replace_empty_structs_in_table(table)
    pyarrow.Table
    """
    for column_name in table.column_names:
        table = replace_empty_structs_in_column(
            table, column_name, dummy_field=dummy_field
        )
    return table


def order_fields_in_struct(column_array, new_struct_type):
    """
    Orders the fields in a struct array to match a new struct type.

    Parameters
    ----------
    column_array : pa.Array
        The original struct array.
    new_struct_type : pa.StructType
        The new struct type with the desired field order.

    Returns
    -------
    pa.Array
        A new struct array with fields ordered according to `new_struct_type`.

    Examples
    --------
    >>> column_array = pa.array([{'b': 2, 'a': 1}], type=pa.struct([pa.field('b', pa.int32()), pa.field('a', pa.int32())]))
    >>> new_struct_type = pa.struct([pa.field('a', pa.int32()), pa.field('b', pa.int32())])
    >>> order_fields_in_struct(column_array, new_struct_type)
    <pyarrow.StructArray object at 0x...>
    """
    # Combine chunks if necessary
    if isinstance(column_array, pa.ChunkedArray):
        column_array = column_array.combine_chunks()

    # Detecting if array is a struct type
    original_type = column_array.type
    if not pa.types.is_struct(original_type):
        return column_array

    new_arrays = []
    for field in new_struct_type:

        field_array = column_array.field(field.name)
        child_type = new_struct_type.field(field.name).type

        if pa.types.is_struct(field.type):
            new_field_array = order_fields_in_struct(field_array, child_type)
        else:
            new_field_array = field_array

        new_arrays.append(new_field_array)

    return pa.StructArray.from_arrays(new_arrays, fields=new_struct_type)


def order_fields_in_table(table, new_schema):
    """
    Orders the fields in a table's struct columns to match a new schema.

    Parameters
    ----------
    table : pa.Table
        The original table.
    new_schema : pa.Schema
        The new schema with the desired field order.

    Returns
    -------
    pa.Table
        A new table with fields ordered according to `new_schema`.

    Examples
    --------
    >>> table = pa.table([{'b': 2, 'a': 1}], schema=pa.schema([pa.field('b', pa.int32()), pa.field('a', pa.int32())]))
    >>> new_schema = pa.schema([pa.field('a', pa.int32()), pa.field('b', pa.int32())])
    >>> order_fields_in_table(table, new_schema)
    pyarrow.Table
    """
    new_columns = []
    for field in new_schema:
        original_column = table.column(field.name)
        if pa.types.is_struct(field.type):
            new_struct_type = field.type
            column_array = order_fields_in_struct(original_column, new_struct_type)
        else:
            column_array = original_column
        new_columns.append(column_array)

    return pa.Table.from_arrays(new_columns, schema=new_schema)


def create_empty_table(
    schema: pa.Schema,
    columns: list = None,
    special_fields: list = [pa.field("id", pa.int64())],
) -> pa.Table:
    """
    Creates an empty PyArrow table with the same schema as the dataset or specific columns.

    Parameters
    ----------
    schema : pa.Schema
        The schema of the dataset to mimic in the empty generator.
    columns : list, optional
        List of column names to include in the empty table. Defaults to None.
    special_fields : list, optional
        A list of fields to use if the schema is empty. Defaults to a field named 'id' of type `pa.int64()`.

    Returns
    -------
    pa.Table
        An empty PyArrow table with the specified schema.

    Examples
    --------
    >>> schema = pa.schema([pa.field('a', pa.int32()), pa.field('b', pa.string())])
    >>> create_empty_table(schema)
    pyarrow.Table
    """
    # If specific columns are provided, filter the schema to include only those columns
    if columns:
        schema = pa.schema([field for field in schema if field.name in columns])

    if not schema.names:
        schema = pa.schema(special_fields)

    # Create an empty table with the derived schema
    empty_table = pa.Table.from_pydict(
        {field.name: [] for field in schema}, schema=schema
    )

    return empty_table


def create_empty_batch_generator(
    schema: pa.Schema,
    columns: list = None,
    special_fields: list = [pa.field("id", pa.int64())],
):
    """
    Orders the fields in a table's struct columns to match a new schema.

    Parameters
    ----------
    table : pa.Table
        The original table.
    new_schema : pa.Schema
        The new schema with the desired field order.

    Returns
    -------
    pa.Table
        A new table with fields ordered according to `new_schema`.

    Examples
    --------
    >>> table = pa.table([{'b': 2, 'a': 1}], schema=pa.schema([pa.field('b', pa.int32()), pa.field('a', pa.int32())]))
    >>> new_schema = pa.schema([pa.field('a', pa.int32()), pa.field('b', pa.int32())])
    >>> order_fields_in_table(table, new_schema)
    pyarrow.Table
    """
    if columns:
        schema = pa.schema([field for field in schema if field.name in columns])

    if not schema.names:
        schema = pa.schema(special_fields)

    yield pa.RecordBatch.from_pydict(
        {field.name: [] for field in schema}, schema=schema
    )


def fill_null_nested_structs(array):
    """
    Fills null values within a nested PyArrow StructArray, recursively processing any nested structs.

    Parameters
    ----------
    array : pa.Array
        The PyArrow StructArray that may contain nested structs with null values.

    Returns
    -------
    pa.StructArray
        A new StructArray with nulls handled recursively within nested structs.

    Examples
    --------
    >>> array = pa.array([{'a': 1, 'b': None}, {'a': None, 'b': {'c': 2}}], type=pa.struct([('a', pa.int32()), ('b', pa.struct([('c', pa.int32())]))]))
    >>> fill_null_nested_structs(array)
    <pyarrow.StructArray object at 0x...>
    """
    array_type = array.type
    child_field_names = [field.name for field in array_type]

    child_chunked_array_list = array.flatten()

    arrays = []
    fields = []
    for child_array, child_field_name in zip(
        child_chunked_array_list, child_field_names
    ):
        child_field_type = child_array.type
        if pa.types.is_struct(child_field_type):
            child_array = fill_null_nested_structs(child_array)
        else:
            child_array = child_array.combine_chunks()

        arrays.append(child_array)
        fields.append(pa.field(child_field_name, child_field_type))

    return pa.StructArray.from_arrays(arrays, fields=fields)


def fill_null_nested_structs_in_table(table):
    """
    Recursively fills null values within nested struct columns of a PyArrow table.

    Parameters
    ----------
    table : pa.Table
        The PyArrow table to process for nested structs and null values.

    Returns
    -------
    pa.Table
        A new table where nulls within nested struct columns have been handled.

    Examples
    --------
    >>> table = pa.table([{'a': 1, 'b': None}, {'a': None, 'b': {'c': 2}}], schema=pa.schema([('a', pa.int32()), ('b', pa.struct([('c', pa.int32())]))]))
    >>> fill_null_nested_structs_in_table(table)
    pyarrow.Table
    """
    column_names = table.column_names
    for column_name in column_names:
        column_array = table.column(column_name)

        if not pa.types.is_struct(column_array.type):
            continue

        column_array = fill_null_nested_structs(column_array)

        # This skips empty structs/dicts
        if len(column_array) != 0:
            table = table.set_column(
                column_names.index(column_name), table.field(column_name), column_array
            )
    return table


def flatten_nested_structs(array, parent_name):
    """
    Flattens nested structs within a PyArrow array, creating fully qualified field names.

    Parameters
    ----------
    array : pa.Array
        The PyArrow StructArray containing nested fields to flatten.
    parent_name : str
        The name of the parent field, used to generate fully qualified field names.

    Returns
    -------
    list of tuple
        A list of tuples, where each tuple contains a flattened array and its corresponding field.

    Examples
    --------
    >>> array = pa.array([{'a': {'b': 1}}, {'a': {'b': 2}}], type=pa.struct([('a', pa.struct([('b', pa.int32())]))]))
    >>> flatten_nested_structs(array, 'a')
    [(array([1, 2], type=int32), Field<name: a.b, type: int32>)]
    """
    array_type = array.type
    child_field_names = [field.name for field in array_type]

    child_chunked_array_list = array.flatten()
    flattened_arrays = []
    for child_array, child_field_name in zip(
        child_chunked_array_list, child_field_names
    ):
        child_field_type = child_array.type

        name = f"{parent_name}.{child_field_name}"
        if pa.types.is_struct(child_field_type):
            flattened_arrays.extend(flatten_nested_structs(child_array, name))
        else:
            flattened_arrays.append((child_array, pa.field(name, child_field_type)))

    return flattened_arrays


def flatten_table_in_column(table, column_name):
    """
    Flattens a nested struct column in a PyArrow Table.

    This function takes a column from a PyArrow Table that is of struct type, and flattens its fields into separate columns.
    The original struct column is replaced by these individual columns in the resulting table. The column names are
    sorted alphabetically after flattening.

    This does not work in in the table_column_callbacks function.
    As it will remove existing field of the nested structs

    Parameters
    ----------
    table : pyarrow.Table
        The input PyArrow Table containing the column to be flattened.
    column_name : str
        The name of the struct column in the table to be flattened.

    Returns
    -------
    pyarrow.Table
        The updated table where the nested struct column has been flattened into individual columns.

    Examples
    --------
    >>> import pyarrow as pa
    >>> data = pa.array([{'a': 1, 'b': 2}, {'a': 3, 'b': 4}], type=pa.struct([('a', pa.int64()), ('b', pa.int64())]))
    >>> table = pa.table([data], names=['col1'])
    >>> modified_table = flatten_table_in_column(table, 'col1')
    >>> print(modified_table)
    pyarrow.Table
    a: int64
    b: int64
    ----
    a: [1, 3]
    b: [2, 4]
    """
    column_type = table.schema.field(column_name).type
    column_names = table.column_names
    column_names.pop(column_names.index(column_name))

    if pa.types.is_struct(column_type):
        logger.debug("Found struct. Trying to flatten")

        column_array = table.column(column_name)

        flattened_arrays_and_fields = flatten_nested_structs(column_array, column_name)

        for flattened_array, flattened_field in flattened_arrays_and_fields:
            table = table.append_column(flattened_field, flattened_array)
            column_names.append(flattened_field.name)

        # Catches case when the column has empty structs
        if len(flattened_arrays_and_fields) == 0:
            column_names.append(column_name)

        sorted_column_names = sorted(column_names)
        table = table.select(sorted_column_names)
    return table


def flatten_table(table):
    """
    Flattens nested struct columns within a PyArrow table.

    Parameters
    ----------
    table : pa.Table
        The PyArrow table containing nested struct columns to flatten.

    Returns
    -------
    pa.Table
        A new table with flattened struct fields.

    Examples
    --------
    >>> table = pa.table([{'a': {'b': 1}}, {'a': {'b': 2}}], schema=pa.schema([('a', pa.struct([('b', pa.int32())]))]))
    >>> flatten_table(table)
    pyarrow.Table
    """

    for column_names in table.column_names:
        table = flatten_table_in_column(table, column_names)
    return table


def _create_null_value_mapping(column):
    """Create mapping between null and non-null values for tensor conversion."""
    # Get mask of valid elements
    valid_mask = column.combine_chunks().is_valid()

    # Create full index array for all elements
    full_indices = pa.array(range(len(valid_mask)))

    # Create sparse mapping with nulls
    sparse_mapping = pc.if_else(valid_mask, full_indices, None)

    # Get indices of non-null elements
    non_null_positions = pc.indices_nonzero(valid_mask)
    dense_indices = pa.array(range(len(non_null_positions)))

    # Create final mapping
    return pc.replace_with_mask(sparse_mapping, valid_mask, dense_indices)


def convert_list_column_to_fixed_tensor(table, column_name):
    """
    Converts a variable-sized list column in a PyArrow Table to a fixed-size list (tensor) column.

    This function checks if a column in the table is of list type and converts it to a fixed-size list array
    (i.e., tensor) based on the dimensions of the first non-null element in the list. The fixed-size list
    array is then updated in the table.

    This will only convert floats, integers, booleans, and decimals. Also it will only convert if the list is homogeneous.

    Parameters
    ----------
    table : pyarrow.Table
        The input PyArrow Table containing the column to be converted.
    column_name : str
        The name of the column in the table which contains list values to be converted to fixed-size arrays.

    Returns
    -------
    pyarrow.Table
        The updated table where the specified column has been converted to a fixed-size list array (tensor).

    Examples
    --------
    >>> import pyarrow as pa
    >>> import numpy as np
    >>> data = pa.array([[1, 2], [3, 4], [5, 6]], type=pa.list_(pa.int64()))
    >>> table = pa.table([data], names=['col1'])
    >>> modified_table = convert_lists_to_fixed_size_list_arrays_in_column(table, 'col1')
    >>> print(modified_table)
    pyarrow.Table
    col1: fixed_shape_tensor<list<item: int64>[2]>
    ----
    col1: [ [1, 2], [3, 4], [5, 6] ]
    """

    try:
        column_type = table.schema.field(column_name).type

        if not pa.types.is_list(column_type):
            return table

        logger.debug(
            f"Found list. Trying to convert Field {column_name} to fixed size list array"
        )

        # Step 1: Extract column array and index
        column_array = table.column(column_name)
        column_index = table.schema.get_field_index(column_name)

        # Step 2: Get first non-null element to determine tensor shape
        non_null_chunk = column_array.drop_null().chunk(0)
        first_element = non_null_chunk[0].values.tolist()
        tensor_shape = np.array(first_element).shape
        tensor_size = math.prod(tensor_shape)

        # Step 3:  Flattens the non-null array, into a single 1-d array combing all rows
        flattened_values = pc.list_flatten(
            column_array, recursive=True
        ).combine_chunks()
        base_type = flattened_values.type

        # Check if the base type is a floating type
        if not (
            pa.types.is_floating(base_type)
            or pa.types.is_integer(base_type)
            or pa.types.is_boolean(base_type)
            or pa.types.is_decimal(base_type)
        ):
            logger.debug(
                "This numpy array is not a floating type. Leaving as ListArray"
            )
            return table

        # Step 4: Create fixed-size array from non-null values
        try:
            fixed_size_array = pa.FixedSizeListArray.from_arrays(
                values=flattened_values, list_size=tensor_size
            )
        except:
            logger.debug(
                f"Inconsistent array sizes detected for {column_name}. Leaving as ListArray"
            )
            return table

        # Step 4: Create mapping for null handling
        null_mapping = _create_null_value_mapping(column_array)

        # Step 5: Reorder the non-null values to match original array structure
        # Example: [[1,2], [3,4]] → [None, [1,2], None, [3,4]]
        reordered_array = fixed_size_array.take(null_mapping)

        # Step 6: Create the tensor type and convert array to tensor format
        tensor_type = pa.fixed_shape_tensor(base_type, tensor_shape)
        tensor_array = pa.ExtensionArray.from_storage(tensor_type, reordered_array)

        # Step 7: Create a null tensor to use for missing values
        # Example: [[None,None]] - this will be used to fill null positions
        null_storage = pa.array(
            [[None] * tensor_size], pa.list_(base_type, tensor_size)
        )
        null_tensor = pa.ExtensionArray.from_storage(tensor_type, null_storage)

        # Step 8: Combine the tensor array with the null tensor
        # Example: [[1,2], [3,4], [None,None]]
        combined_tensors = pa.concat_arrays([tensor_array, null_tensor])

        # Step 9: Create mapping to place nulls and values in correct positions
        valid_mask = null_mapping.is_valid()  # Shows which positions should be null
        array_len = len(valid_mask)

        # Index of the null tensor in combined array (it's at the end)
        null_index = pa.array([array_len])
        # Indices for non-null values
        value_indices = pa.array(range(array_len))
        # Create mapping: null positions get None, valid positions get their index
        value_mapping = pc.if_else(valid_mask, value_indices, None)

        # Step 10: Replace None values with index of null tensor and apply mapping
        # Example mapping: [2, 0, 2, 1] → points to null tensor (2) or value indices (0,1)
        final_mapping = value_mapping.fill_null(null_index[0])
        tensor_array = combined_tensors.take(final_mapping)

        # Step 11: Update table with new tensor column
        tensor_field = pa.field(column_name, tensor_array.type)
        return table.set_column(column_index, tensor_field, tensor_array)
    except Exception as e:

        logger.debug(
            f"Error converting {column_name} list column to fixed tensor. Leaving as ListArray"
        )
        return table


def table_column_callbacks(table, callbacks=[]):
    """
    Applies a list of callback functions to each column in a PyArrow Table.

    This function iterates over all columns in the provided table and applies each callback function from the
    `callbacks` list to each column. The callbacks are expected to modify the table in some way (e.g., transforming
    or updating columns), and the updated table is returned after all callbacks are applied.

    Parameters
    ----------
    table : pyarrow.Table
        The input PyArrow Table to which the callback functions will be applied.
    callbacks : list of callable, optional
        A list of functions to be applied to each column in the table. Each callback should take two arguments:
        the table and the name of the column.

    Returns
    -------
    pyarrow.Table
        The updated table after applying all callback functions to each column.

    Examples
    --------
    >>> import pyarrow as pa
    >>> def uppercase_column_names(table, column_name):
    ...     new_name = column_name.upper()
    ...     column = table.column(column_name)
    ...     return table.rename_columns([new_name if name == column_name else name for name in table.column_names])
    ...
    >>> data = pa.array([1, 2, 3])
    >>> table = pa.table([data], names=['col1'])
    >>> modified_table = table_column_callbacks(table, callbacks=[uppercase_column_names])
    >>> print(modified_table)
    pyarrow.Table
    COL1: int64
    ----
    COL1: [1, 2, 3]
    """
    for column_name in table.column_names:
        logger.info(f"Applying callbacks to column: {column_name}")
        for callback in callbacks:
            logger.debug(f"Applying callback: {callback.__name__}")
            table = callback(table, column_name)
    return table


def create_struct_arrays_from_dict(nested_dict):
    """
    Creates PyArrow StructArrays and schema from a nested dictionary.

    Parameters
    ----------
    nested_dict : dict
        The dictionary where keys represent field names and values are either arrays or nested dictionaries.

    Returns
    -------
    tuple of (pa.StructArray, pa.StructType)
        A tuple containing the created StructArray and its corresponding StructType schema.

    Examples
    --------
    >>> nested_dict = {'a': pa.array([1, 2]), 'b': {'c': pa.array([3, 4])}}
    >>> create_struct_arrays_from_dict(nested_dict)
    (<pyarrow.StructArray object at 0x...>, StructType(a: int64, b: StructType(c: int64)))
    """
    arrays = []
    fields = []
    field_names = []
    for name, value in nested_dict.items():
        # logger.debug(f"Building nest for field: {name}")
        if isinstance(value, dict):
            array, struct = create_struct_arrays_from_dict(value)
            field = pa.field(name, struct)
        else:
            array = value
            if isinstance(array, pa.ChunkedArray):
                array = array.combine_chunks()
            field = pa.field(name, value.type)

        arrays.append(array)
        fields.append(field)
        field_names.append(name)
    return pa.StructArray.from_arrays(arrays, fields=fields), pa.struct(fields)


def create_nested_arrays_dict_from_flattened_table(table):
    """
    Reconstructs a nested dictionary of arrays from a flattened PyArrow table.

    Parameters
    ----------
    table : pa.Table
        The PyArrow table with flattened field names.

    Returns
    -------
    dict
        A dictionary where keys represent the nested field structure, and values are the corresponding arrays.

    Examples
    --------
    >>> table = pa.table([pa.array([1, 2]), pa.array([3, 4])], names=['a.b', 'a.c'])
    >>> create_nested_arrays_dict_from_flattened_table(table)
    {'a': {'b': <pyarrow.Array object at 0x...>, 'c': <pyarrow.Array object at 0x...>}}
    """
    # Get the column names
    columns = table.column_names

    # Create a dictionary to store the nested structure
    nested_fields = {}
    nested_arrays = {}
    for col in columns:
        parts = col.split(".")
        current = nested_fields
        current_arrays = nested_arrays
        for i, part in enumerate(parts):
            if i == len(parts) - 1:
                current[part] = table.field(col).type
                current_arrays[part] = table.column(col)
            else:
                if part not in current:
                    current[part] = {}
                    current_arrays[part] = {}
                current = current[part]
                current_arrays = current_arrays[part]

    return nested_arrays


def rebuild_nested_table(table, load_format="table"):
    nested_arrays_dict = create_nested_arrays_dict_from_flattened_table(table)
    nested_arrays, new_struct = create_struct_arrays_from_dict(nested_arrays_dict)
    new_schema = pa.schema(new_struct, metadata=table.schema.metadata)
    if load_format == "table":
        return pa.Table.from_arrays(nested_arrays.flatten(), schema=new_schema)
    elif load_format == "batches":
        return pa.RecordBatch.from_arrays(nested_arrays.flatten(), schema=new_schema)


def update_flattend_table(
    current_table, incoming_table, update_keys: Union[List[str], str] = ["id"]
):
    """
    Updates the current table using the values from the incoming table by flattening both
    tables, applying the updates, and then rebuilding the nested structure.

    Parameters
    ----------
    current_table : pa.Table
        The current PyArrow table to update.
    incoming_table : pa.Table
        The incoming PyArrow table containing updated values.

    Returns
    -------
    pa.Table
        The updated PyArrow table with flattened and rebuilt structure.
    """

    logger.debug("Updating table with the flatten method")
    logger.debug(f"Current table shape: {current_table.shape}")
    logger.debug(f"Incoming table shape: {incoming_table.shape}")

    incoming_schema = incoming_table.schema
    current_schema = current_table.schema
    merged_schema = unify_schemas(
        [current_schema, incoming_schema], promote_options="default"
    )

    current_field_names = current_schema.names
    incoming_field_names = incoming_schema.names

    # Aligning current and incoming tables with merged schema
    aligned_incoming_table = table_schema_cast(incoming_table, merged_schema)
    aligned_current_table = table_schema_cast(current_table, merged_schema)

    # Generate an index mask for the current table, identifying the positions of matching 'id' values in the incoming table.
    # The index_mask will align with the number of rows in the current table, marking where matching ids exist in the incoming table.

    if isinstance(update_keys, str):
        index_mask = pc.index_in(
            aligned_current_table[update_keys], aligned_incoming_table[update_keys]
        )
    elif isinstance(update_keys, list) and len(update_keys) == 1:
        index_mask = pc.index_in(
            aligned_current_table[update_keys[0]],
            aligned_incoming_table[update_keys[0]],
        )
    elif isinstance(update_keys, list) and len(update_keys) > 1:

        tmp_current_table = aligned_current_table.select(update_keys)
        tmp_incoming_table = aligned_incoming_table.select(update_keys)
        tmp_current_table = tmp_current_table.append_column(
            "current_index", pa.array(np.arange(len(tmp_current_table)))
        )
        tmp_incoming_table = tmp_incoming_table.append_column(
            "incoming_index", pa.array(np.arange(len(tmp_incoming_table)))
        )

        right_outer_table = tmp_incoming_table.join(
            tmp_current_table,
            keys=update_keys,
            right_keys=update_keys,
            join_type="right outer",
        )
        index_mask = right_outer_table["incoming_index"].take(
            right_outer_table["current_index"]
        )

    # Create an update table by selecting rows from the incoming table using the index mask.
    update_table = pc.take(aligned_incoming_table, index_mask)

    logger.debug(f"update_table shape: {update_table.shape}")
    # Default the updated table to the current table
    updated_table = aligned_current_table

    for column_name in aligned_current_table.column_names:
        logger.debug(f"Looking for updates in field: {column_name}")
        # Do not update the id column
        if isinstance(update_keys, str) and column_name == update_keys:
            continue
        if isinstance(update_keys, list) and column_name in update_keys:
            continue
        if column_name == "id":
            continue

        # Do not update the column if it is not in the incoming table
        if column_name not in incoming_field_names:
            continue

        current_array = aligned_current_table[column_name]
        update_array = update_table[column_name]

        if isinstance(current_array, pa.ChunkedArray):
            current_array = current_array.combine_chunks()
        if isinstance(update_array, pa.ChunkedArray):
            update_array = update_array.combine_chunks()

        if not is_extension_type(update_array.type):
            # Attempt to fill null values in update_array with values from current_array
            updated_array = update_array.fill_null(current_array)
        else:  # is_fixed_shape_tensor(update_array.type):
            # If the above operation fails, proceed with manual handling for fixed-size tensors

            # Get boolean masks for valid (non-null) and null entries in update_array
            is_valid_array = update_array.is_valid()
            is_null_array = update_array.is_null()

            sum_is_valid_array = pc.sum(is_valid_array)
            if sum_is_valid_array == pa.scalar(0, type=sum_is_valid_array.type):
                logger.debug(
                    f"No updates are present or non-null for column: {column_name}"
                )
                continue

            # Create a sequence array for indexing
            sequence = pa.array(range(len(is_valid_array)))

            # Generate an index mask where valid entries have their indices, and null entries are None
            is_valid_index_mask = pc.if_else(is_valid_array, sequence, None)

            # Get indices of non-null values in update_array
            indices = pc.indices_nonzero(is_valid_array)

            # Create an array of indices for non-null values
            non_null_indices = pa.array(range(len(indices)))

            # Replace valid indices in the mask with indices of non-null values
            is_valid_index_mask = pc.replace_with_mask(
                is_valid_index_mask, is_valid_array, non_null_indices
            )

            # Generate unique indices for the update_array portion in combined_arrays
            filter_update_array_sequence = pa.array(
                range(len(is_valid_array), 2 * len(is_valid_array))
            )
            update_is_valid_index_mask = pc.if_else(
                is_valid_array, filter_update_array_sequence, None
            )

            # Generate indices for null values in current_array
            filter_current_array_sequence = pa.array(range(len(is_valid_array)))
            current_is_valid_index_mask = pc.if_else(
                is_null_array, filter_current_array_sequence, None
            )

            # Combine the two masks, filling nulls in current mask with update mask values
            combined_mask = current_is_valid_index_mask.fill_null(
                update_is_valid_index_mask
            )
            # Concatenate current_array and update_array
            combined_arrays = pa.concat_arrays([current_array, update_array])

            # Select elements from combined_arrays based on the combined_mask
            updated_array = combined_arrays.take(combined_mask)

        # Update the column in updated_table with the new updated_array
        updated_table = updated_table.set_column(
            aligned_current_table.column_names.index(column_name),
            aligned_current_table.field(column_name),
            updated_array,
        )
    updated_table = updated_table.sort_by("id")
    return updated_table


def infer_pyarrow_types(data_dict: dict):
    """
    Infers PyArrow types for the given dictionary of data. The function skips the 'id' field and infers
    the data types for all other keys.

    Parameters
    ----------
    data_dict : dict
        A dictionary where keys represent field names and values represent data values.

    Returns
    -------
    dict
        A dictionary where keys are field names and values are the inferred PyArrow data types.

    Examples
    --------
    >>> data_dict = {'a': 123, 'b': 'string_value', 'id': 1}
    >>> infer_pyarrow_types(data_dict)
    {'a': DataType(int64), 'b': DataType(string)}
    """
    infered_types = {}
    for key, value in data_dict.items():
        if key != "id":
            infered_types[key] = pa.infer_type([value])
    return infered_types


def update_schema(current_schema, schema=None, field_dict=None, update_metadata=True):
    """
    Update the schema of a given table based on a provided schema or field modifications.

    This function allows updating the schema of a PyArrow table by either replacing the entire schema
    or modifying individual fields within the existing schema. It can take a dictionary of field
    names and their corresponding new field definitions to update specific fields in the schema.
    Alternatively, a completely new schema can be provided to replace the current one.

    Parameters
    ----------
    current_current : pa.Schema
        The current schema of the table.
    schema : pa.Schema, optional
        A new schema to replace the existing schema of the table. If provided, this will
        completely override the current schema.
    field_dict : dict, optional
        A dictionary where the keys are existing field names and the values are the new
        PyArrow field definitions to replace the old ones. This is used for selectively
        updating specific fields within the current schema.
    update_metadata : bool, optional
        Whether to update the metadata of the table.

    Returns
    -------
    pa.Table
        A new PyArrow table with the updated schema.
    """
    # Check if the table name is in the list of table names
    updated_metadata = {}
    if current_schema.metadata:
        updated_metadata.update(current_schema.metadata)
    current_field_names = sorted(current_schema.names)

    logger.debug(f"current_field_names: {current_field_names}")
    logger.debug(f"current_metadata: {updated_metadata}")
    if field_dict:
        updated_schema = current_schema
        for field_name, new_field in field_dict.items():
            field_index = current_schema.get_field_index(field_name)

            if field_name in current_field_names:
                updated_schema = updated_schema.set(field_index, new_field)

    if schema is not None:
        updated_schema = schema
        if schema.metadata and update_metadata:
            updated_metadata.update(schema.metadata)
        else:
            updated_metadata = schema.metadata

    field_names = []
    for field in current_field_names:
        field_names.append(updated_schema.field(field))
    updated_schema = pa.schema(field_names, metadata=updated_metadata)
    return updated_schema


def unify_schemas(schema_list, promote_options="permissive"):
    current_schema, new_schema = schema_list
    merged_metadata = {}
    table_metadata_equal = current_schema.metadata == new_schema.metadata
    if not table_metadata_equal:
        if current_schema.metadata:
            merged_metadata.update(current_schema.metadata)
        if new_schema.metadata:
            merged_metadata.update(new_schema.metadata)
    merged_schema = pa.unify_schemas(
        [current_schema, new_schema], promote_options=promote_options
    )
    merged_schema = merged_schema.remove_metadata()
    merged_schema = merged_schema.with_metadata(merged_metadata)
    return merged_schema


def align_table(current_table: pa.Table, new_schema: pa.Schema) -> pa.Table:
    """
    Aligns the given table to the new schema, filling in missing fields or struct fields with null values.

    Args:
        table (pa.Table): The table to align.
        new_schema (pa.Schema): The target schema to align the table to.

    Returns:
        pa.Table: The aligned table.
    """
    # current_table=replace_empty_structs_in_table(current_table)

    current_table = add_new_null_fields_in_table(current_table, new_schema)

    current_table = order_fields_in_table(current_table, new_schema)

    return current_table


def add_new_null_fields_in_column(column_array, field, new_type):
    column_type = column_array.type
    logger.debug(f"Field name:  {field.name}")
    logger.debug(f"Column type: {column_type}")
    logger.debug(f"New type: {new_type}")

    if pa.types.is_struct(new_type):
        logger.debug("This column is a struct")
        # Replacing empty structs with dummy structs
        new_type_names = [field.name for field in new_type]
        if field.name in new_type_names:
            new_struct_type = new_type.field(field.name).type
        else:
            new_struct_type = new_type
        new_struct_type = merge_structs(new_struct_type, column_type)
        logger.debug(f"New struct type: {new_struct_type}")
        new_array = add_new_null_fields_in_struct(column_array, new_struct_type)
        new_field = pa.field(field.name, new_array.type)
        return new_array, new_field
    else:
        logger.debug("This column is not a struct")
        return column_array, field


def add_new_null_fields_in_table(table, new_schema):
    new_columns_fields = []
    new_columns = []
    for field in new_schema:
        if field.name not in table.schema.names:
            new_column = pa.nulls(table.num_rows, type=field.type)
            new_field = pa.field(field.name, field.type)
        else:
            original_column = table.column(field.name)
            new_column, new_field = add_new_null_fields_in_column(
                original_column, field, field.type
            )

        new_columns.append(new_column)
        new_columns_fields.append(new_field)

    return pa.Table.from_arrays(new_columns, schema=new_schema)


def add_new_null_fields_in_struct(column_array, new_struct_type):

    # Combine chunks if necessary
    if isinstance(column_array, pa.ChunkedArray):
        column_array = column_array.combine_chunks()

    # Detecting if array is a struct type
    original_type = column_array.type
    if not pa.types.is_struct(original_type):
        return column_array

    original_fields_dict = {field.name: i for i, field in enumerate(original_type)}

    new_arrays = []
    for field in new_struct_type:
        if field.name in original_fields_dict:
            logger.debug("Adding values to a existing field")
            # Recursively generate the new array for the field
            field_array = column_array.field(original_fields_dict[field.name])
            new_field_array = add_new_null_fields_in_struct(
                field_array, field_array.type
            )
            new_arrays.append(new_field_array)
        else:
            logger.debug("Adding null values to a previously non-existing field")
            null_array = pa.nulls(len(column_array), field.type)
            new_arrays.append(null_array)
    return pa.StructArray.from_arrays(new_arrays, fields=new_struct_type)


def table_schema_cast(current_table, new_schema):
    current_names = set(current_table.column_names)
    new_names = set(new_schema.names)

    all_names = current_names.union(new_names)
    all_names_sorted = sorted(all_names)

    new_minus_current = new_names - current_names
    current_intersection_new = current_names.intersection(new_names)

    for name in current_intersection_new:
        logger.debug(f"Fields in both current and incomring tables: {name}")

        if pa.types.is_list(current_table.column(name).type):
            try:
                column_array = current_table.column(name).cast(
                    new_schema.field(name).type
                )
                column_array.validate()
            except pa.lib.ArrowInvalid as e:
                raise Exception(
                    f"\n\n Error casting column {name} to type {new_schema.field(name).type}.\n\n"
                    "This is typically caused by a nested list field containing an array of nulls.\n"
                    "Either remove the null field before putting the data in ParquetDB or \n"
                    "provide the exact schema when first introducing the new field"
                )

        current_table = current_table.set_column(
            current_table.schema.get_field_index(name),
            new_schema.field(name),
            current_table.column(name).cast(new_schema.field(name).type),
        )

    for name in new_minus_current:
        logger.debug(f"Found new field: {name}")
        new_type = new_schema.field(name).type

        if (
            hasattr(new_type, "extension_name")
            and new_type.extension_name == "arrow.fixed_shape_tensor"
        ):
            logger.debug(f"Field {name} is a fixed shape tensor")
            tensor_size = 1
            for dim in new_type.shape:
                tensor_size *= dim

            base_type = new_type.value_type
            tensor_shape = new_type.shape
            tensor_type = pa.fixed_shape_tensor(base_type, tensor_shape)
            null_storage = pa.array(
                [[None] * tensor_size], pa.list_(base_type, tensor_size)
            )
            null_tensor = pa.ExtensionArray.from_storage(tensor_type, null_storage)

            null_column = [null_tensor] * len(current_table)
            current_table = current_table.append_column(
                new_schema.field(name), null_column
            )
            continue

        current_table = current_table.append_column(
            new_schema.field(name),
            pa.nulls(len(current_table), type=new_schema.field(name).type),
        )

    current_table = current_table.select(all_names_sorted)

    current_table = current_table.replace_schema_metadata(new_schema.metadata)
    return current_table


def schema_equal(schema1, schema2):
    are_schemas_equal = schema1.equals(schema2)
    logger.debug(f"Are schemas equal before metadata check: {are_schemas_equal}")
    # Check if metadata are equal
    metadata1 = {}
    if schema1.metadata:
        metadata1.update(schema1.metadata)
    metadata2 = {}
    if schema2.metadata:
        metadata2.update(schema2.metadata)

    if metadata1 != metadata2:
        logger.info(f"Metadata not equal. Current: {metadata1}, Incoming: {metadata2}")
        are_schemas_equal = False
    return are_schemas_equal


def sort_schema(schema):

    names = set(schema.names)
    names_sorted = sorted(names)

    field_names = []
    for name in names_sorted:
        field_names.append(schema.field(name))
    return pa.schema(field_names, metadata=schema.metadata)


def join_tables(
    left_table,
    right_table,
    left_keys,
    right_keys=None,
    join_type="left outer",
    left_suffix=None,
    right_suffix=None,
    coalesce_keys=True,
):
    # Add index column to both tables
    if right_keys:
        tmp_right_table = right_table.select(right_keys)
        tmp_right_table = tmp_right_table.append_column(
            "right_index", pa.array(np.arange(len(tmp_right_table)))
        )
    else:
        tmp_right_table = pa.Table.from_arrays(
            [pa.array(np.arange(len(right_table)))], names=["right_index"]
        )

    tmp_left_table = left_table.select(left_keys)
    tmp_left_table = tmp_left_table.append_column(
        "left_index", pa.array(np.arange(len(tmp_left_table)))
    )

    joined_table = tmp_left_table.join(
        tmp_right_table,
        keys=left_keys,
        right_keys=right_keys,
        join_type=join_type,
        coalesce_keys=coalesce_keys,
    )

    right_columns = set(right_table.column_names)
    left_columns = set(left_table.column_names)

    metadata = {}
    try:
        for column_name in left_columns:
            field = left_table.schema.field(column_name)
            if column_name in left_keys:
                continue
            new_column = left_table[column_name].take(joined_table["left_index"])
            if (
                left_suffix
                and column_name in right_columns
                and column_name in left_columns
                and "right_index" in joined_table.column_names
            ):
                column_name = column_name + left_suffix

            new_field = field.with_name(column_name)
            joined_table = joined_table.add_column(0, new_field, new_column)

        left_metadata = left_table.schema.metadata
        if left_metadata:
            metadata.update(left_metadata)
    except Exception as e:
        logger.debug(e)
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

        right_metadata = right_table.schema.metadata
        if right_metadata:
            metadata.update(right_metadata)
    except Exception as e:
        logger.debug(e)
        pass

    if "right_index" in joined_table.column_names:
        joined_table = joined_table.drop(["right_index"])
    if "left_index" in joined_table.column_names:
        joined_table = joined_table.drop(["left_index"])

    joined_table = joined_table.replace_schema_metadata(metadata)
    return joined_table


def update_fixed_shape_tensor(current_array, update_array):
    is_valid_array = update_array.is_valid()
    is_null_array = update_array.is_null()

    sum_is_valid_array = pc.sum(is_valid_array)
    if sum_is_valid_array == pa.scalar(0, type=sum_is_valid_array.type):
        logger.debug("No updates are present or non-null for column: {column_name}")
        return current_array

    # Create a sequence array for indexing
    sequence = pa.array(range(len(is_valid_array)))

    # Generate an index mask where valid entries have their indices, and null entries are None
    is_valid_index_mask = pc.if_else(is_valid_array, sequence, None)

    # Get indices of non-null values in update_array
    indices = pc.indices_nonzero(is_valid_array)

    # Create an array of indices for non-null values
    non_null_indices = pa.array(range(len(indices)))

    # Replace valid indices in the mask with indices of non-null values
    is_valid_index_mask = pc.replace_with_mask(
        is_valid_index_mask, is_valid_array, non_null_indices
    )

    # Generate unique indices for the update_array portion in combined_arrays
    filter_update_array_sequence = pa.array(
        range(len(is_valid_array), 2 * len(is_valid_array))
    )
    update_is_valid_index_mask = pc.if_else(
        is_valid_array, filter_update_array_sequence, None
    )

    # Generate indices for null values in current_array
    filter_current_array_sequence = pa.array(range(len(is_valid_array)))
    current_is_valid_index_mask = pc.if_else(
        is_null_array, filter_current_array_sequence, None
    )

    # Combine the two masks, filling nulls in current mask with update mask values
    combined_mask = current_is_valid_index_mask.fill_null(update_is_valid_index_mask)
    # Concatenate current_array and update_array
    combined_arrays = pa.concat_arrays([current_array, update_array])

    # Select elements from combined_arrays based on the combined_mask
    updated_array = combined_arrays.take(combined_mask)
    return updated_array


def is_extension_type(type):
    return hasattr(type, "extension_name")


def is_fixed_shape_tensor(type):
    return is_extension_type(type) and type.extension_name == "arrow.fixed_shape_tensor"
