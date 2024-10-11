import pyarrow as pa
import logging

from parquetdb.utils.general_utils import timeit
import pyarrow.compute as pc

logger = logging.getLogger(__name__)

# https://arrow.apache.org/docs/python/api/datatypes.html
t_string=pa.string()
t_int32=pa.int32()
t_int64=pa.int64()
t_float32=pa.float32()
t_float64=pa.float64()
t_bool=pa.bool_()

# Create variable-length or fixed size binary type.
t_binary = pa.binary()

#one of ‘s’ [second], ‘ms’ [millisecond], ‘us’ [microsecond], or ‘ns’ [nanosecond]
t_timestamp=pa.timestamp('ms')


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

def merge_structs(current_type: pa.StructType, incoming_type: pa.StructType) -> pa.StructType:
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
            if pa.types.is_struct(incoming_field.type) and pa.types.is_struct(current_field.type):
                merged_field = pa.field(
                    incoming_field.name,
                    merge_structs(current_field.type, incoming_field.type)
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



@timeit
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
    
            if pa.types.is_struct(current_field.type) and pa.types.is_struct(incoming_field.type):
                # If both fields are structs, merge them recursively
                merged_field = pa.field(
                    current_field.name,
                    merge_structs(current_field.type, incoming_field.type)
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

def replace_empty_structs(column_array: pa.Array, dummy_field=pa.field('dummy_field', pa.int16())):
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
    
    # Catches non struct field cases
    if not pa.types.is_struct(column_array.type):
        return column_array
    
    # Catches empty structs cases
    if len(column_array.type)==0:
        null_array = pa.nulls(len(column_array), dummy_field.type)
        return pc.make_struct(null_array, field_names=[dummy_field.name])
    
    
    child_field_names=[field.name for field in column_array.type]
    child_chunked_array_list = column_array.flatten()
    
    child_arrays=[]
    for child_array, child_field_name in zip(child_chunked_array_list, child_field_names):
        child_array=replace_empty_structs(child_array)
        child_arrays.append(child_array)
    

    return pc.make_struct(*child_arrays, field_names=child_field_names)
    
def replace_empty_structs_in_table(table, dummy_field=pa.field('dummy_field', pa.int16())):
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
    for field_name in table.column_names:
        field_index=table.schema.get_field_index(field_name)
        
        field=table.field(field_index)
        column_array = table.column(field_index)
        
        if pa.types.is_struct(column_array.type):
            logger.debug('This column is a struct')
            # Replacing empty structs with dummy structs
            new_column_array=replace_empty_structs(column_array, dummy_field=dummy_field)
            new_field=pa.field(field_name, new_column_array.type)
        else:
            new_column_array=column_array
            new_field=field
            
        table = table.set_column(field_index,
                                 new_field, 
                                 new_column_array)
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

def create_empty_table(schema: pa.Schema, columns: list = None, special_fields: list = [pa.field('id', pa.int64())]) -> pa.Table:
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

    logger.debug(f"Schema: \n{schema}\n")

    if not schema.names:
        schema=pa.schema(special_fields)

    
    # Create an empty table with the derived schema
    empty_table = pa.Table.from_pydict({field.name: [] for field in schema}, schema=schema)

    return empty_table

def create_empty_batch_generator(schema: pa.Schema, 
                                 columns: list = None, 
                                 special_fields: list = [pa.field('id', pa.int64())]):
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
        schema=pa.schema(special_fields)    
        
    yield pa.RecordBatch.from_pydict({field.name: [] for field in schema}, schema=schema)

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
    child_field_names=[field.name for field in array_type]

    child_chunked_array_list = array.flatten()
    
    arrays=[]
    fields=[]
    for child_array, child_field_name in zip(child_chunked_array_list, child_field_names):
        child_field_type=child_array.type
        if pa.types.is_struct(child_field_type):
            child_array=fill_null_nested_structs(child_array)
        else:
            child_array=child_array.combine_chunks()
            
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
    column_names=table.column_names
    for column_name in column_names:
        column_array=table.column(column_name)
        
        if not pa.types.is_struct(column_array.type):
            continue
        
        column_array=fill_null_nested_structs(column_array)
        
        # This skips empty structs/dicts
        if len(column_array)!=0:
            table=table.set_column(column_names.index(column_name), table.field(column_name), column_array)
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
    child_field_names=[field.name for field in array_type]

    child_chunked_array_list = array.flatten()
    flattened_arrays=[]
    for child_array, child_field_name in zip(child_chunked_array_list, child_field_names):
        child_field_type=child_array.type

        name = f"{parent_name}.{child_field_name}"
        if pa.types.is_struct(child_field_type):
            flattened_arrays.extend(flatten_nested_structs(child_array, name))
        else:
            flattened_arrays.append((child_array, pa.field(name, child_field_type)))
            
    return flattened_arrays

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

    flattened_columns = []
    flattened_fields = []

    for i, column in enumerate(table.columns):
        column_name = table.field(i).name
        if pa.types.is_struct(column.type):
            flattened_arrays_and_fields = flatten_nested_structs(column, column_name)
            
            # This is to handle empty structs
            if len(flattened_arrays_and_fields)==0:
                flattened_columns.append(column)
                flattened_fields.append(pa.field(column_name, column.type))
                continue
            
            flattened_column, flattend_fields = zip(*flattened_arrays_and_fields)
            
   
            flattened_columns.extend(flattened_column)
            flattened_fields.extend(flattend_fields)

        else:
            flattened_columns.append(column)
            flattened_fields.append(pa.field(column_name, column.type))
    table=pa.Table.from_arrays(flattened_columns, schema=pa.schema(flattened_fields, metadata=table.schema.metadata))
    # table=table.select(sorted(table.column_names))
    return table


@timeit
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
    arrays=[]
    fields=[]
    field_names=[]
    for name, value in nested_dict.items():
        if isinstance(value, dict):
            array, struct = create_struct_arrays_from_dict(value)
            field=pa.field(name, struct)
        else:
            array=value
            array=array.combine_chunks()
            field=pa.field(name,value.type)
        
        arrays.append(array)
        fields.append(field)
        field_names.append(name)
    return pa.StructArray.from_arrays(arrays, fields=fields), pa.struct(fields)

@timeit
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
    nested_arrays={}
    for col in columns:
        parts = col.split('.')
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

@timeit
def rebuild_nested_table(table):
    nested_arrays_dict = create_nested_arrays_dict_from_flattened_table(table)
    nested_arrays, new_struct = create_struct_arrays_from_dict(nested_arrays_dict)
    new_schema=pa.schema(new_struct)
    return pa.Table.from_arrays(nested_arrays.flatten(), schema=new_schema)

def update_flattend_table(current_table, incoming_table):
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

    Examples
    --------
    >>> updated_table = update_table_flatten_method(current_table, incoming_table)
    pyarrow.Table
    """
    
    logger.debug("Updating table with the flatten method")

    for column_name in current_table.column_names:
        logger.debug(f"Looking for updates in field: {column_name}")
        update_array=update_table_column(current_table, incoming_table, column_name=column_name)
        field_index=current_table.schema.get_field_index(column_name)
        if update_array and len(update_array)!=0:
            logger.info(f"Updating column: {column_name}")
            current_table=current_table.set_column(field_index, current_table.field(column_name), update_array)
    return current_table



def update_table_column(current_table, incoming_table, column_name):
    """
    Updates a specific column in the current table with values from the incoming table, 
    based on matching 'id' fields. Non-null values in the incoming table will replace 
    corresponding values in the current table.

    Parameters
    ----------
    current_table : pa.Table
        The current PyArrow table to update.
    incoming_table : pa.Table
        The incoming PyArrow table containing updated values.
    column_name : str
        The name of the column to update in the current table.

    Returns
    -------
    pa.Array or None
        The updated column array if updates are present and non-null; otherwise, returns None.

    Examples
    --------
    >>> update_table_column(current_table, incoming_table, 'column1')
    <pyarrow.Array object at 0x...>
    """
    logger.debug(f"Updating column: {column_name}")

    incoming_filter = pc.field('id').isin(current_table['id']) & ~pc.field(column_name).is_null(incoming_table[column_name])
    filtered_incoming_table = incoming_table.filter(incoming_filter)
    updates_are_present_and_not_null = filtered_incoming_table.num_rows != 0

    if not updates_are_present_and_not_null:
        logger.debug("No updates are present or non-null")
        return None
    
    # Creating a boolean mask
    current_mask = pc.is_in(current_table['id'], value_set=filtered_incoming_table['id']).combine_chunks()
    current_array = current_table[column_name].combine_chunks()
    incoming_array = filtered_incoming_table[column_name].combine_chunks()
    
    updated_array = pc.replace_with_mask(current_array, current_mask, incoming_array)

    return updated_array

def update_table_flatten_method(current_table, incoming_table):
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

    Examples
    --------
    >>> updated_table = update_table_flatten_method(current_table, incoming_table)
    pyarrow.Table
    """
    
    logger.debug("Updating table with the flatten method")
    current_table=flatten_table(current_table)
    incoming_table=flatten_table(incoming_table)
    
    for column_name in current_table.column_names:
        logger.debug(f"Looking for updates in field: {column_name}")
        update_array=update_table_column(current_table, incoming_table, column_name=column_name)
        field_index=current_table.schema.get_field_index(column_name)
        if update_array and len(update_array)!=0:
            logger.info(f"Updating column: {column_name}")
            current_table=current_table.set_column(field_index, current_table.field(column_name), update_array)
    current_table=rebuild_nested_table(current_table)

    return current_table

def update_struct_child_field(current_table, incoming_table, field_path):
    """
    Updates a nested child field within a struct column in the current table based on 
    values from the incoming table.

    Parameters
    ----------
    current_table : pa.Table
        The current PyArrow table to update.
    incoming_table : pa.Table
        The incoming PyArrow table containing updated values.
    field_path : list of str
        The path to the nested field inside the struct.

    Returns
    -------
    pa.Array or None
        The updated nested field array if updates are present and non-null; otherwise, returns None.

    Examples
    --------
    >>> update_struct_child_field(current_table, incoming_table, ['parent_field', 'child_field'])
    <pyarrow.Array object at 0x...>
    """
    logger.debug(f"field_path: {field_path}")

    parent_name=field_path[0]
    sub_path=field_path[1:]
    
    logger.debug(f"parent_name: {parent_name}")
    logger.debug(f"sub_path: {sub_path}")
    
    
    incoming_filter=pc.field('id').isin(current_table['id']) & ~pc.field(*field_path).is_null(incoming_table[parent_name])
    
    filtered_incoming_table = incoming_table.filter(incoming_filter)

    updates_are_present_and_not_null = filtered_incoming_table.num_rows != 0
    
    
    if not updates_are_present_and_not_null:
        logger.debug("Updates are not present and null")
        return None
    
    # Creating boolean mask
    current_mask = pc.is_in(current_table['id'], value_set=filtered_incoming_table['id']).combine_chunks()
    
    current_array = pc.struct_field(current_table[parent_name], sub_path).combine_chunks()
    incoming_array = pc.struct_field(filtered_incoming_table[parent_name], sub_path).combine_chunks()
    
    # filtered_array = pc.filter(mask, mask)
    # logger.debug(f"Values where the array is True: {len(filtered_array)}")
    logger.debug(f"Mask shape: {len(current_mask)}")
    logger.debug(f"Incoming array shape: {len(incoming_array)}")
    logger.debug(f"Current array shape: {len(current_array)}")
    
    new_array= pc.replace_with_mask(current_array, current_mask, incoming_array)
    return new_array

def update_field(current_table, incoming_table, field_name):
    """
    Updates a specific field in the current table with values from the incoming table,
    based on matching 'id' fields.

    Parameters
    ----------
    current_table : pa.Table
        The current PyArrow table to update.
    incoming_table : pa.Table
        The incoming PyArrow table containing updated values.
    field_name : str
        The name of the field to update in the current table.

    Returns
    -------
    pa.Array or None
        The updated field array if updates are present and non-null; otherwise, returns None.

    Examples
    --------
    >>> update_field(current_table, incoming_table, 'field_name')
    <pyarrow.Array object at 0x...>
    """
    logger.debug(f"field_name: {field_name}")
    
    incoming_filter=pc.field('id').isin(current_table['id']) & ~pc.field(field_name).is_null(incoming_table[field_name])
   
    filtered_incoming_table = incoming_table.filter(incoming_filter)

    updates_are_present_and_not_null = filtered_incoming_table.num_rows != 0
    if not updates_are_present_and_not_null:
        logger.debug("Updates are not present and not null")
        return None
    
    # Creating boolean mask
    current_mask = pc.is_in(current_table['id'], value_set=filtered_incoming_table['id']).combine_chunks()
    
    current_array=current_table[field_name].combine_chunks()
    incoming_array = filtered_incoming_table[field_name].combine_chunks()
    
    # filtered_array = pc.filter(mask, mask)
    # logger.debug(f"Values where the array is True: {len(filtered_array)}")
    logger.debug(f"Mask shape: {len(current_mask)}")
    logger.debug(f"Incoming array shape: {len(incoming_array)}")
    logger.debug(f"Current array shape: {len(current_array)}")
    
    new_array = pc.replace_with_mask(current_array,current_mask,incoming_array)
    return new_array

def update_struct_field(current_table, incoming_table, field_path, current_array=None):
    """
    Recursively updates nested fields in a struct column of the current table using values 
    from the incoming table.

    Parameters
    ----------
    current_table : pa.Table
        The current PyArrow table to update.
    incoming_table : pa.Table
        The incoming PyArrow table containing updated values.
    field_path : list of str
        The path to the nested field inside the struct.
    current_array : pa.Array, optional
        The current struct array being processed. Defaults to None.

    Returns
    -------
    pa.StructArray
        The updated struct array with nested fields updated.

    Examples
    --------
    >>> update_struct_field(current_table, incoming_table, ['parent_field', 'child_field'])
    <pyarrow.StructArray object at 0x...>
    """
    if current_array is None:
        logger.debug("This is the first call to update_nested_field")
        current_array=current_table[field_path[0]]
        
    child_field_names=[field.name for field in current_array.type]
    
    logger.debug(f"child_field_names: {child_field_names}")
    

    child_chunked_array_list = current_array.flatten()
    child_arrays=[]
    for child_array, child_field_name in zip(child_chunked_array_list, child_field_names):
        child_field_type=child_array.type
        
        sub_path=field_path.copy()
        sub_path.append(child_field_name)

        if pa.types.is_struct(child_field_type):
            update_array=update_struct_field(current_table, incoming_table, sub_path, current_array=child_array)
        else:
            update_array=update_struct_child_field(current_table, incoming_table, sub_path)
        
        if update_array:
            logger.debug(f"update_array is None for field: {child_field_name}")
            
            child_arrays.append(update_array)
        else:
            logger.debug(f"update_array is not None for field: {child_field_name}")
            child_arrays.append(child_array.combine_chunks())

    return pc.make_struct(*child_arrays, field_names=child_field_names)

def update_table_nested_method(current_table, incoming_table):
    """
    Updates the current table with values from the incoming table using a nested field update approach.
    If a field is a struct, it will recursively update the nested fields. Otherwise, it updates the field directly.

    Parameters
    ----------
    current_table : pa.Table
        The current PyArrow table to update.
    incoming_table : pa.Table
        The incoming PyArrow table containing updated values.

    Returns
    -------
    pa.Table
        The updated PyArrow table with the changes from the incoming table.

    Examples
    --------
    >>> updated_table = update_table_nested_method(current_table, incoming_table)
    pyarrow.Table
    """
    logger.debug("Updating table with nested method")
    for field_name in current_table.column_names:
        logger.debug(f"Looking for updates in field: {field_name}")
        if pa.types.is_struct(current_table.schema.field(field_name).type):
            
            # Process nested struct fields
            updated_array=update_struct_field(current_table, incoming_table, [field_name])
        else:
            # Process non-struct fields
            updated_array=update_field(current_table, incoming_table, field_name)
        
        if updated_array and len(updated_array)!=0:
            logger.info(f"Updating field: {field_name}")
            current_table=current_table.set_column(current_table.schema.get_field_index(field_name), 
                                                current_table.schema.field(field_name), 
                                                updated_array)
    return current_table

@timeit
def update_table(current_table, incoming_table, flatten_method=False):
    """
    Updates the current table using either a flatten method or a nested method depending on the `flatten_method` flag.
    
    Parameters
    ----------
    current_table : pa.Table
        The current PyArrow table to update.
    incoming_table : pa.Table
        The incoming PyArrow table containing updated values.
    flatten_method : bool, optional
        If True, the flatten method will be used; otherwise, the nested method is used. 
        Defaults to False.

    Returns
    -------
    pa.Table
        The updated PyArrow table after applying the changes from the incoming table.

    Examples
    --------
    >>> updated_table = update_table(current_table, incoming_table, flatten_method=True)
    pyarrow.Table
    """
    logger.info("Updating table")
    if flatten_method:
        current_table=update_table_flatten_method(current_table, incoming_table)
    else:
        current_table=update_table_nested_method(current_table, incoming_table)
        
    return current_table


def infer_pyarrow_types(self, data_dict: dict):
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
        if key != 'id':
            infered_types[key] = pa.infer_type([value])
    return infered_types


def update_schema(current_schema, schema=None, field_dict=None):
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

    Returns
    -------
    pa.Table
        A new PyArrow table with the updated schema.
    """
    # Check if the table name is in the list of table names
    current_field_names=sorted(current_schema.names)
    if field_dict:
        updated_schema=current_schema
        for field_name, new_field in field_dict.items():
            field_index=current_schema.get_field_index(field_name)

            if field_name in current_field_names:
                updated_schema=updated_schema.set(field_index, new_field)

    if schema:
        updated_schema=schema
        
    field_names=[]
    for field in current_field_names:
        field_names.append(updated_schema.field(field))
    updated_schema=pa.schema(field_names, metadata=current_schema.metadata)

    return updated_schema


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

    current_table=add_new_null_fields_in_table(current_table, new_schema)
    
    current_table=order_fields_in_table(current_table, new_schema)
    
    return current_table

def add_new_null_fields_in_column(column_array, field, new_type):    
    column_type = column_array.type
    logger.debug(f"Field name:  {field.name}")
    logger.debug(f"Column type: {column_type}")
    logger.debug(f"New type: {new_type}")
    
    if pa.types.is_struct(new_type):
        logger.debug('This column is a struct')
        # Replacing empty structs with dummy structs
        new_type_names=[field.name for field in new_type]
        if field.name in new_type_names:
            new_struct_type=new_type.field(field.name).type
        else:
            new_struct_type=new_type
        new_struct_type = merge_structs(new_struct_type,column_type)
        logger.debug(f"New struct type: {new_struct_type}")
        new_array=add_new_null_fields_in_struct(column_array, new_struct_type)
        new_field=pa.field(field.name, new_array.type)
        return new_array, new_field
    else:
        logger.debug('This column is not a struct')
        return column_array, field

def add_new_null_fields_in_table(table, new_schema):
    new_columns_fields=[]
    new_columns=[]
    for field in new_schema:
        if field.name not in table.schema.names:
            new_column=pa.nulls(table.num_rows, type=field.type)
            new_field=pa.field(field.name, field.type)  
        else:
            original_column = table.column(field.name)
            new_column, new_field = add_new_null_fields_in_column(original_column, field, field.type)

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

    new_arrays=[]
    for field in new_struct_type:
        if field.name in original_fields_dict:
            logger.debug("Adding values to a existing field")
            # Recursively generate the new array for the field
            field_array = column_array.field(original_fields_dict[field.name])
            new_field_array = add_new_null_fields_in_struct(field_array, field_array.type)
            new_arrays.append(new_field_array)
        else:
            logger.debug("Adding null values to a previously non-existing field")
            null_array = pa.nulls(len(column_array), field.type)
            new_arrays.append(null_array)
    return pa.StructArray.from_arrays(new_arrays, fields=new_struct_type)


def table_schema_cast(current_table, new_schema):
    current_names=set(current_table.column_names)
    new_names=set(new_schema.names)

    all_names=current_names.union(new_names)
    all_names_sorted=sorted(all_names)

    new_minus_current = new_names - current_names
    current_intersection_new = current_names.intersection(new_names)
    
    for name in current_intersection_new:
        current_table=current_table.set_column(current_table.schema.get_field_index(name), 
                                               new_schema.field(name), 
                                               current_table.column(name).cast(new_schema.field(name).type))

    for name in new_minus_current:
        current_table=current_table.append_column(new_schema.field(name), pa.nulls(len(current_table), type=new_schema.field(name).type))

    
    current_table=current_table.select(all_names_sorted)
    return current_table



