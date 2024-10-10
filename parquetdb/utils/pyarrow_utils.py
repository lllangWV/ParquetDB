import pyarrow as pa
import logging

from parquetdb.utils.general_utils import timeit
import pyarrow.compute as pc


import cProfile
import pstats
import io
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

    Args:
        schema1 (pyarrow.Schema): The first schema.
        schema2 (pyarrow.Schema): The second schema.

    Returns:
        set: A set of field names that are in schema1 but not in schema2.
    """
    # Create a set of field names from the first schema
    field_names1 = set(schema1)
    # Create a set of field names from the second schema
    field_names2 = set(schema2)
    # Find the difference between the two sets
    difference = field_names1.difference(field_names2)
    return difference

def merge_structs(current_type: pa.StructType, incoming_type: pa.StructType) -> pa.StructType:
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

def schema_to_struct(schema):
    return pa.struct(schema)

@timeit
def merge_schemas(current_schema: pa.Schema, incoming_schema: pa.Schema) -> pa.Schema:
    
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

def add_null_columns_for_missing_fields(table: pa.Table, new_schema: pa.Schema) -> pa.Table:
    """
    Adds null columns for any fields that are missing in the new schema.

    Args:
        table (pa.Table): The table to add null columns to.
        new_schema (pa.Schema): The schema of the new table.

    Returns:
        pa.Table: The table with null columns added for missing fields.
    """
    table_schema = table.schema
    field_names_missing = list(set(new_schema.names) - set(table_schema.names))
    
    # Add missing fields with null values
    for new_field_name in field_names_missing:
        field_type = new_schema.field(new_field_name).type
        null_array = pa.nulls(table.num_rows, type=field_type)
        table = table.append_column(new_field_name, null_array)
    
    return table

def align_struct_fields(original_array: pa.Array, new_type: pa.DataType, dummy_type=pa.struct([('dummy_field', pa.int16())])) -> pa.Array:
    """
    Aligns the fields of an array to match the new type, filling missing fields with null values.
    Handles nested struct types recursively and replaces empty structs with a dummy struct.

    Args:
        original_array (pa.Array): The original array (can be a struct or any other type).
        new_type (pa.DataType): The target type to align to.

    Returns:
        pa.Array: The aligned array.
    """
    
    # Combine chunks if necessary
    if isinstance(original_array, pa.ChunkedArray):
        original_array = pa.concat_arrays(original_array.chunks)
    
    # If the new type is not a struct, just cast and return
    if not isinstance(new_type, pa.StructType):
        return original_array.cast(new_type)

    dummy_field_name = [field.name for field in dummy_type][-1]

    original_type = original_array.type
    original_fields_dict = {field.name: i for i, field in enumerate(original_type)}
    
    # Build a new struct with all the required fields
    new_arrays=[]
    for field in new_type:
        if field.name in original_fields_dict:
            # If the field exists, align it recursively
            original_field = original_array.field(original_fields_dict[field.name])
            aligned_field = align_struct_fields(original_field, field.type)
            
            # Check if the aligned field is an empty struct
            if isinstance(field.type, pa.StructType) and len(field.type) == 0:
                # Replace empty struct with dummy struct
                dummy_array = pa.array([{dummy_field_name: None}] * len(original_array), type=dummy_type)
                new_arrays.append(dummy_array)
            else:
                new_arrays.append(aligned_field)
        else:
            # If the field doesn't exist, fill with nulls
            if isinstance(field.type, pa.StructType) and len(field.type) == 0:
                # For empty struct fields, use the dummy struct
                dummy_array = pa.array([{dummy_field_name: None}] * len(original_array), type=dummy_type)
                new_arrays.append(dummy_array)
            else:
                null_array = pa.nulls(len(original_array), field.type)
                new_arrays.append(null_array)
    
    return pa.StructArray.from_arrays(new_arrays, fields=new_type)

@timeit
def align_table(current_table: pa.Table, new_schema: pa.Schema) -> pa.Table:
    """
    Aligns the given table to the new schema, filling in missing fields or struct fields with null values.

    Args:
        table (pa.Table): The table to align.
        new_schema (pa.Schema): The target schema to align the table to.

    Returns:
        pa.Table: The aligned table.
    """
    current_table=replace_empty_structs_in_table(current_table)

    current_table=add_new_null_fields_in_table(current_table, new_schema)
    
    current_table=order_fields_in_table(current_table, new_schema)
    
    return current_table

def combine_tables(current_table: pa.Table, incoming_table: pa.Table, merged_schema: pa.Schema=None) -> pa.Table:
    """
    Combines the current table and incoming table by aligning them to the merged schema and filling missing fields with nulls.

    Args:
        current_table (pa.Table): The current table.
        incoming_table (pa.Table): The incoming table.
        merged_schema (pa.Schema): The merged schema.

    Returns:
        pa.Table: The combined table.
    """
    if merged_schema is None:
        current_schema = current_table.schema
        incoming_schema = incoming_table.schema
        merged_schema = merge_schemas(current_schema, incoming_schema)

    current_table=align_table(current_table, merged_schema)
    incoming_table=align_table(incoming_table, merged_schema)

    # Reorder the columns to match the merged schema
    current_table = current_table.select(merged_schema.names)
    incoming_table = incoming_table.select(merged_schema.names)

    # Combine the tables
    combined_table = pa.concat_tables([current_table, incoming_table])
    
    return combined_table

def replace_none_with_nulls(data, schema_field):
    """
    Replaces None values in the data list with null values according to the schema.

    Args:
        data (list): A list of elements to check.
        schema_field (pa.Field or pa.DataType): The field or type that defines the schema.

    Returns:
        list: The updated list with None values replaced with nulls.
    """
    # Check if the schema_field is a pa.Field or pa.DataType
    schema_type = schema_field.type if isinstance(schema_field, pa.Field) else schema_field

    updated_data = []

    for element in data:
        if element is None:
            # Replace None with the equivalent null value
            null_value = pa.scalar(None, type=schema_type)
            updated_data.append(null_value.as_py())
        elif pa.types.is_struct(schema_type):
            # If the field is a struct, recursively check its fields
            updated_data.append(
                {key: replace_none_with_nulls([val], schema_type.field(key))[0] 
                 if val is None else val for key, val in element.items()}
            )
        elif pa.types.is_list(schema_type):
            # If the field is a list, recursively check the elements of the list
            updated_data.append(replace_none_with_nulls(element, schema_type.value_type))
        else:
            # Otherwise, keep the element as is
            updated_data.append(element)

    return updated_data

def replace_empty_structs(struct_type, dummy_field=pa.field('dummy_field', pa.int16())):
    dummy_struct_type=pa.struct([dummy_field])
    # If the struct is empty, return the dummy struct
    if len(struct_type)==0:
        return dummy_struct_type

    field_list=[]
    # Iterate over the fields in the struct
    for field in struct_type:
        field_name=field.name
        # Handles the determination of the field type
        if pa.types.is_struct(field.type):
            # Handles empty structs.
            if len(field.flatten())==0:
                field_type=dummy_struct_type
            # Handles nested structs.
            else:
                field_type=replace_empty_structs(field.type)
        else:
            field_type=field.type

        field_list.append((field_name,field_type))

    return pa.struct(field_list)

def replace_empty_structs_in_struct(column_array: pa.Array, dummy_field=pa.field('dummy_field', pa.int16())) -> pa.Array:
    dummy_struct_type=pa.struct([dummy_field])

    # Combine chunks if necessary
    if isinstance(column_array, pa.ChunkedArray):
        column_array = column_array.combine_chunks()

    # Detecting if array is a struct type
    original_type = column_array.type
    if pa.types.is_struct(original_type):
        # Adding dummy field to the struct type
        new_type = replace_empty_structs(original_type)
    else:
        # If the array is not a struct type, return the original array
        return column_array

    original_fields_dict = {field.name: i for i, field in enumerate(original_type)}

    # Build a new struct with all the required fields
    new_arrays=[]
    for field in new_type:
        if field.name in original_fields_dict:
            logger.debug("Adding values to a existing field")
            # Recursively generate the new array for the field
            field_array = column_array.field(original_fields_dict[field.name])
            new_field_array = replace_empty_structs_in_struct(field_array)
            new_arrays.append(new_field_array)
        else:
            logger.debug("Adding null values to a previously non-existing field")
            null_array = pa.nulls(len(column_array), field.type)
            new_arrays.append(null_array)
    return pa.StructArray.from_arrays(new_arrays, fields=new_type)

def replace_empty_structs_in_column(column_array, field, dummy_type=pa.field('dummy_field', pa.int16())):    
    column_type = column_array.type
    logger.debug(f"Field name: {field.name}")
    logger.debug(f"Column type: {column_type}")
    logger.debug(f"Dummy_type type: {dummy_type}")
    if pa.types.is_struct(column_type):
        logger.debug('This column is a struct')
        # Replacing empty structs with dummy structs
        new_array=replace_empty_structs_in_struct(column_array)
        new_field=pa.field(field.name, new_array.type)
        return new_array, new_field
    else:
        logger.debug('This column is not a struct')
        # If the column is not a struct type, return the original column
        return column_array, field

def replace_empty_structs_in_table(table, dummy_type=pa.field('dummy_field', pa.int16())):
    for col_idx in range(table.num_columns):
        field=table.field(col_idx)
        column_array = table.column(col_idx)

        column_array, field = replace_empty_structs_in_column(column_array, field, dummy_type=dummy_type)
        table = table.set_column(col_idx, field, column_array)
    return table

def add_new_null_fields_in_struct(column_array, new_struct_type):
    # Combine chunks if necessary
    if isinstance(column_array, pa.ChunkedArray):
        column_array = column_array.combine_chunks()

    # Detecting if array is a struct type
    original_type = column_array.type
    if not pa.types.is_struct(original_type):
        print(column_array.type)
        logger.debug(f"Column is not a struct type. Returning original column array")
        print(column_array)
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

def add_new_null_fields_in_column(column_array, field):    
    column_type = column_array.type
    logger.debug(f"Field name:  {field.name}")
    logger.debug(f"Column type: {column_type}")
    logger.debug(f"New type: {field.type}")
    
    if pa.types.is_struct(column_type):
        logger.debug('This column is a struct')
        # Replacing empty structs with dummy structs
        new_type_names=[field.name for field in field.type]
        if field.name in new_type_names:
            new_struct_type=field.type.field(field.name).type
        else:
            new_struct_type=field.type
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
            new_column, new_field = add_new_null_fields_in_column(original_column, field)

        new_columns.append(new_column)
        new_columns_fields.append(new_field)
    table = pa.Table.from_arrays(new_columns, schema=pa.schema(new_columns_fields))
    return table

def order_fields_in_struct(column_array, new_struct_type):
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

def order_fields_in_column(column_array, field):
    # Combine chunks if necessary
    if pa.types.is_struct(field.type):
        new_struct_type = field.type
        column_array = order_fields_in_struct(column_array, new_struct_type)
    else:
        column_array = column_array
    return column_array

def order_fields_in_table(table, new_schema):
    new_columns = []
    for field in new_schema:
        original_column = table.column(field.name)
        column_array=order_fields_in_column(original_column, field)
        new_columns.append(column_array)

    return pa.Table.from_arrays(new_columns, schema=new_schema)

def merge_tables(current_table: pa.Table, incoming_table: pa.Table, schema=None) -> pa.Table:
    """
    Combines the current table and incoming table by aligning them to the merged schema and filling missing fields with nulls.

    Args:
        current_table (pa.Table): The current table.
        incoming_table (pa.Table): The incoming table.
        merged_schema (pa.Schema): The merged schema.

    Returns:
        pa.Table: The combined table.
    """
    current_table=replace_empty_structs_in_table(current_table)
    incoming_table=replace_empty_structs_in_table(incoming_table)

    if schema is None:
        current_schema = current_table.schema
        incoming_schema = incoming_table.schema
        merged_schema = merge_schemas(current_schema, incoming_schema)

    current_table=add_new_null_fields_in_table(current_table, merged_schema)
    incoming_table=add_new_null_fields_in_table(incoming_table, merged_schema)

    current_table=order_fields_in_table(current_table, merged_schema)
    incoming_table=order_fields_in_table(incoming_table, merged_schema)

    # # Combine the tables
    combined_table = pa.concat_tables([current_table, incoming_table])
    
    return combined_table

def create_empty_table(schema: pa.Schema, columns: list = None, special_fields: list = [pa.field('id', pa.int64())]) -> pa.Table:
    """
    Creates an empty PyArrow table with the same schema as the dataset or specific columns.

    Args:
        schema (pa.Schema): The schema of the dataset to mimic in the empty generator.
        columns (list, optional): List of column names to include in the empty table. Defaults to None.

    Returns:
        pa.Table: An empty PyArrow table with the specified schema.
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
    Returns an empty generator that yields nothing.

    Args:
        schema (pa.Schema): The schema of the dataset to mimic in the empty generator.
        columns (list, optional): List of column names to include in the empty table. Defaults to None.
    Yields:
        pa.RecordBatch: Empty record batches with the specified schema.
    """
    if columns:
        schema = pa.schema([field for field in schema if field.name in columns])
        
    if not schema.names:
        schema=pa.schema(special_fields)    
        
    yield pa.RecordBatch.from_pydict({field.name: [] for field in schema}, schema=schema)

def fill_null_nested_structs(array):
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




@timeit
def flatten_nested_chunked_arrays(array, parent_name):
    array_type = array.type
    child_field_names=[field.name for field in array_type]

    child_chunked_array_list = array.flatten()
    flattened_arrays=[]
    for child_array, child_field_name in zip(child_chunked_array_list, child_field_names):
        child_field_type=child_array.type

        name = f"{parent_name}.{child_field_name}"
        if pa.types.is_struct(child_field_type):
            flattened_array=flatten_nested_chunked_arrays(child_array, name)
            flattened_arrays.extend(flatten_nested_chunked_arrays(child_array, name))
        else:
            flattened_arrays.append((child_array, pa.field(name, child_field_type)))
            
    return flattened_arrays

@timeit
def flatten_table(table):
    flattened_columns = []
    flattened_fields = []

    for i, column in enumerate(table.columns):
        column_name = table.field(i).name
        if pa.types.is_struct(column.type):
            flattened_arrays_and_fields = flatten_nested_chunked_arrays(column, column_name)
            
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
    
    return pa.Table.from_arrays(flattened_columns, schema=pa.schema(flattened_fields))

def struct_from_dict(type_dict):
    fields=[]
    for name, value in type_dict.items():
        if isinstance(value, dict):
            field=pa.field(name, struct_from_dict(value))
        else:
            field=pa.field(name, value)
            
        fields.append(field)
    return pa.struct(fields)

@timeit
def struct_arrays_from_dict(nested_dict):
    arrays=[]
    fields=[]
    field_names=[]
    for name, value in nested_dict.items():
        if isinstance(value, dict):
            array, struct = struct_arrays_from_dict(value)
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
def get_nested_type_dict_from_flattened_table(table):
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
                
    return nested_fields, nested_arrays

@timeit
def rebuild_nested_table(table):
    type_dict, nested_arrays_dict = get_nested_type_dict_from_flattened_table(table)
    nested_arrays, new_struct = struct_arrays_from_dict(nested_arrays_dict)
    new_schema=pa.schema(new_struct)
    return pa.Table.from_arrays(nested_arrays.flatten(), schema=new_schema)

def update_table_column(current_table, incoming_table, column_name):
    """Update a the current table column with the values from the incoming table. 
    First it will check if the incoming ids are in the current table. 
    Then it it will filter the incoming table for only the ids and non-null values

    This function updates assumes the incoming and current tables have the same schema.
    It also assumes that the incoming and current tables have a fully flattened schema.

    """
    profiler = cProfile.Profile()
    profiler.enable()

    logger.debug(f"Updating column: {column_name}")

    # Start profiling this section
    profiler.enable()
    incoming_filter = pc.field('id').isin(current_table['id']) & ~pc.field(column_name).is_null(incoming_table[column_name])
    filtered_incoming_table = incoming_table.filter(incoming_filter)
    updates_are_present_and_not_null = filtered_incoming_table.num_rows != 0

    profiler.disable()
    if not updates_are_present_and_not_null:
        logger.debug("No updates are present or non-null")
        return None
    
    profiler.enable()
    # Creating a boolean mask
    current_mask = pc.is_in(current_table['id'], value_set=filtered_incoming_table['id']).combine_chunks()
    
    profiler.disable()
    profiler.enable()
    current_array = current_table[column_name].combine_chunks()
    incoming_array = filtered_incoming_table[column_name].combine_chunks()
    
    profiler.disable()
    profiler.enable()
    updated_array = pc.replace_with_mask(current_array, current_mask, incoming_array)
    
    profiler.disable()
    
    # Log and output profiling results
    s = io.StringIO()
    ps = pstats.Stats(profiler, stream=s).sort_stats('tottime')
    ps.print_stats()

    with open('data/profile_results.log', 'w') as f:
        f.write(s.getvalue())
    
    return updated_array

def update_table_flatten_method(current_table, incoming_table):
    """
    This method will update the current table with the values from the incoming table.
    It will update the current table by flattening the both the current and incoming tables, 
    applying the update, then rebuilding the nested structure
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

def update_struct_field(current_table, incoming_table, field_path):
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

def update_nested_field(current_table, incoming_table, field_path, current_array=None):
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
            update_array=update_nested_field(current_table, incoming_table, sub_path, current_array=child_array)
        else:
            update_array=update_struct_field(current_table, incoming_table, sub_path)
        
        if update_array:
            logger.debug(f"update_array is None for field: {child_field_name}")
            
            child_arrays.append(update_array)
        else:
            logger.debug(f"update_array is not None for field: {child_field_name}")
            child_arrays.append(child_array.combine_chunks())

    return pc.make_struct(*child_arrays, field_names=child_field_names)

def update_table_nested_method(current_table, incoming_table):
    logger.debug("Updating table with nested method")
    for field_name in current_table.column_names:
        logger.debug(f"Looking for updates in field: {field_name}")
        if pa.types.is_struct(current_table.schema.field(field_name).type):
            
            # Process nestedstruct fields
            updated_array=update_nested_field(current_table, incoming_table, [field_name])
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
    logger.info("Updating table")
    if flatten_method:
        current_table=update_table_flatten_method(current_table, incoming_table)
    else:
        current_table=update_table_nested_method(current_table, incoming_table)
        
    return current_table