import pyarrow as pa

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

def align_struct_fields(original_array: pa.ChunkedArray, new_type: pa.StructType) -> pa.ChunkedArray:
    """
    Aligns the struct fields of an array to match the new schema, filling missing fields with null values.

    Args:
        original_array (pa.ChunkedArray): The original struct array.
        new_type (pa.StructType): The target struct type to align to.

    Returns:
        pa.ChunkedArray: The aligned struct array.
    """
    # Combine chunks if necessary
    original_array = pa.concat_arrays(original_array.chunks) if isinstance(original_array, pa.ChunkedArray) else original_array
    
    original_type = original_array.type
    original_fields_dict = {field.name: i for i, field in enumerate(original_type)}
    
    # Build a new struct with all the required fields
    new_arrays = []
    for field in new_type:
        if field.name in original_fields_dict:
            # If the field exists, keep the original field
            new_arrays.append(original_array.field(original_fields_dict[field.name]))
        else:
            # Otherwise, fill with nulls
            null_array = pa.nulls(len(original_array), field.type)
            new_arrays.append(null_array)
    
    return pa.StructArray.from_arrays(new_arrays, fields=new_type)


def align_table(table: pa.Table, new_schema: pa.Schema) -> pa.Table:
    """
    Aligns the given table to the new schema, filling in missing fields or struct fields with null values.

    Args:
        table (pa.Table): The table to align.
        new_schema (pa.Schema): The target schema to align the table to.

    Returns:
        pa.Table: The aligned table.
    """
    # Add missing top-level columns
    table = add_null_columns_for_missing_fields(table, new_schema)

    # Align any struct fields
    for field in new_schema:
        if pa.types.is_struct(field.type):
            original_column = table.column(field.name)
            aligned_column = align_struct_fields(original_column, field.type)
            table = table.set_column(table.schema.get_field_index(field.name), field.name, aligned_column)

    # Reorder the columns to match the new schema
    table = table.select(new_schema.names)
    
    return table

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