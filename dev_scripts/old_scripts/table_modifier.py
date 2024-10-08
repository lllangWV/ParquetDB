
from typing import List, Callable, Dict

import pyarrow as pa
import copy
import pandas as pd

pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', None)  # Auto-detect display width
pd.set_option('display.max_colwidth', None)  # Show full content of each column

from parquetdb.utils import pyarrow_utils


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
            print("Adding values to a existing field")
            # Recursively generate the new array for the field
            field_array = column_array.field(original_fields_dict[field.name])
            new_field_array = replace_empty_structs_in_struct(field_array)
            new_arrays.append(new_field_array)
        else:
            print("Adding null values to a previously non-existing field")
            null_array = pa.nulls(len(column_array), field.type)
            new_arrays.append(null_array)
    return pa.StructArray.from_arrays(new_arrays, fields=new_type)

def replace_empty_structs_in_column(column_array, field, dummy_type=pa.field('dummy_field', pa.int16())):    
    column_type = column_array.type
    print("Field name: ", field.name)
    print("Column type: ", column_type)
    print("Dummy_type type: ", dummy_type)
    if pa.types.is_struct(column_type):
        print('This column is a struct')
        # Replacing empty structs with dummy structs
        new_array=replace_empty_structs_in_struct(column_array)
        new_field=pa.field(field.name, new_array.type)
        return new_array, new_field
    else:
        print('This column is not a struct')
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
        return column_array

    original_fields_dict = {field.name: i for i, field in enumerate(original_type)}

    new_arrays=[]
    for field in new_struct_type:
        if field.name in original_fields_dict:
            print("Adding values to a existing field")
            # Recursively generate the new array for the field
            field_array = column_array.field(original_fields_dict[field.name])
            new_field_array = add_new_null_fields_in_struct(field_array, field_array.type)
            new_arrays.append(new_field_array)
        else:
            print("Adding null values to a previously non-existing field")
            null_array = pa.nulls(len(column_array), field.type)
            new_arrays.append(null_array)
    return pa.StructArray.from_arrays(new_arrays, fields=new_struct_type)

def add_new_null_fields_in_column(column_array, field, new_type):    
    column_type = column_array.type
    print("Field name: ", field.name)
    print("Column type: ", column_type)
    print("New type: ", new_type)
    
    if pa.types.is_struct(column_type):
        print('This column is a struct')
        # Replacing empty structs with dummy structs
        new_type_names=[field.name for field in new_type]
        if field.name in new_type_names:
            new_struct_type=new_type.field(field.name).type
        else:
            new_struct_type=new_type
        new_struct_type = pyarrow_utils.merge_structs(new_struct_type,column_type)
        print("New struct type: ", new_struct_type)
        new_array=add_new_null_fields_in_struct(column_array, new_struct_type)
        new_field=pa.field(field.name, new_array.type)
        return new_array, new_field
    else:
        print('This column is not a struct')
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

def merge_tables(current_table: pa.Table, incoming_table: pa.Table) -> pa.Table:
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

    current_schema = current_table.schema
    incoming_schema = incoming_table.schema
    merged_schema = pyarrow_utils.merge_schemas(current_schema, incoming_schema)

    current_table=add_new_null_fields_in_table(current_table, merged_schema)
    incoming_table=add_new_null_fields_in_table(incoming_table, merged_schema)

    current_table=order_fields_in_table(current_table, merged_schema)
    incoming_table=order_fields_in_table(incoming_table, merged_schema)

    # # Combine the tables
    combined_table = pa.concat_tables([current_table, incoming_table])
    
    return combined_table

if __name__ == "__main__":

    
    data_1 = [
            {'a': 1, 'b': {'x': {'x': {}, 'y': 20}, 'y': 20}, 'c': {}},
            {'a': 2, 'b': {'x': {'x': {}, 'y': 20}, 'y': 40}, 'c': {}},
        ]

    data_2 = [
            { 'b': {'z': 20}, 'd':1}
        ]


    # data = [
    #         { 'c':  {'x': {'x': {}, 'y': 20} }},
    #         {'c':  {'x': {'x': {}, 'y': 20} }},
    #     ]
    # print(data)
    table_1=pa.Table.from_pylist(data_1)
    table_2=pa.Table.from_pylist(data_2)

    # new_schema=pyarrow_utils.merge_schemas(table_1.schema, table_2.schema)
    # print(new_schema)
    # table_1=replace_empty_structs_in_table(table_1)

    # table_1=add_new_null_fields_in_table(table_1, new_schema)
    # df_1 = table_1.to_pandas()
    # print(df_1.columns)
    # print(df_1.head())

    merged_table=merge_tables(table_1, table_2)

    df_merged = merged_table.to_pandas()
    print(df_merged.columns)
    print(df_merged.head())
