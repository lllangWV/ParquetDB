import logging
import random
import os
import shutil
import time
import tempfile
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from glob import glob
import json

import pyarrow as pa

from parquetdb import ParquetDB, ParquetDatasetDB
from parquetdb.pyarrow_utils import align_table, merge_schemas, merge_structs, combine_tables

logger=logging.getLogger('parquetdb')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

##########################################################################################################################
# materials_dir = "C:/Users/lllang/Desktop/Current_Projects/ParquetDB/data/raw/materials_data"
# files = glob(os.path.join(materials_dir, '*.json'))

# # Load materials data from JSON files
# materials_data = []
# for file in files:
#     with open(file, 'r') as f:
#         materials_data.append(json.load(f))
##########################################################################################################################

import pyarrow as pa


# def process_type(data_type):
#     if pa.types.is_struct(data_type):
#         fields = [field for field in struct_type]

#         if len(fields) == 0:
#             # Create a dummy field for empty structs
#             dummy_field = pa.field('dummy', pa.null())
#             return pa.struct([dummy_field])
#         else:
#             # Recursively process each field in the struct
#             new_fields = [pa.field(field.name, process_type(field.type), field.nullable, field.metadata)
#                           for field in fields]
#             return pa.struct(new_fields)
#     elif pa.types.is_list(data_type):
#         # Process the value type of the list
#         value_type = process_type(data_type.value_type)
#         return pa.list_(value_type)
#     elif pa.types.is_large_list(data_type):
#         value_type = process_type(data_type.value_type)
#         return pa.large_list(value_type)
#     elif pa.types.is_map(data_type):
#         # Process the key and item types of the map
#         key_type = process_type(data_type.key_type)
#         item_type = process_type(data_type.item_type)
#         return pa.map_(key_type, item_type)
#     elif pa.types.is_union(data_type):
#         # Process each child type of the union
#         new_fields = [pa.field(field.name, process_type(field.type), field.nullable, field.metadata)
#                       for field in data_type]
#         return pa.union(new_fields, data_type.mode)
#     else:
#         # Return the data type as is for other types
#         return data_type

# def process_schema(schema):
#     # Recursively process the schema to modify data types
#     new_fields = [pa.field(field.name, process_type(field.type), field.nullable, field.metadata)
#                   for field in schema]
#     return pa.schema(new_fields, metadata=schema.metadata)

# def process_array(array, old_type, new_type):
#     if old_type == new_type:
#         return array
#     elif pa.types.is_struct(old_type):
#         if pa.types.is_struct(new_type):
#             old_fields = {field.name: field for field in old_type}
#             new_fields = {field.name: field for field in new_type}
#             child_arrays = []
#             for field_name in new_fields:
#                 if field_name in old_fields:
#                     old_child_array = array.field(field_name)
#                     child_array = process_array(old_child_array,
#                                                 old_fields[field_name].type,
#                                                 new_fields[field_name].type)
#                 else:
#                     # Create a null array for the new dummy field
#                     length = len(array)
#                     child_array = pa.nulls(length, type=new_fields[field_name].type)
#                 child_arrays.append(child_array)
#             # Create a new StructArray with updated fields
#             return pa.StructArray.from_arrays(child_arrays, fields=list(new_fields.values()))
#         else:
#             raise TypeError("Type mismatch: old type is struct, new type is not struct")
#     elif pa.types.is_list(old_type):
#         if pa.types.is_list(new_type):
#             old_value_type = old_type.value_type
#             new_value_type = new_type.value_type
#             value_array = array.values
#             new_value_array = process_array(value_array, old_value_type, new_value_type)
#             return pa.ListArray.from_arrays(array.offsets, new_value_array)
#         else:
#             raise TypeError("Type mismatch: old type is list, new type is not list")
#     elif pa.types.is_large_list(old_type):
#         if pa.types.is_large_list(new_type):
#             old_value_type = old_type.value_type
#             new_value_type = new_type.value_type
#             value_array = array.values
#             new_value_array = process_array(value_array, old_value_type, new_value_type)
#             return pa.LargeListArray.from_arrays(array.offsets, new_value_array)
#         else:
#             raise TypeError("Type mismatch: old type is large list, new type is not large list")
#     else:
#         # For other types, attempt to cast
#         try:
#             return array.cast(new_type)
#         except:
#             raise TypeError(f"Cannot cast array of type {old_type} to {new_type}")

# def process_chunked_array(chunked_array, old_type, new_type):
#     new_chunks = [process_array(chunk, old_type, new_type) for chunk in chunked_array.chunks]
#     return pa.chunked_array(new_chunks, type=new_type)

# def add_dummy_field_to_empty_structs(table):
#     # Process the schema to get the new schema
#     new_schema = process_schema(table.schema)
#     new_columns = []
#     for i, field in enumerate(table.schema):
#         old_type = field.type
#         new_type = new_schema.field(i).type
#         chunked_array = table.column(i)
#         # Process each column's data to match the new schema
#         new_chunked_array = process_chunked_array(chunked_array, old_type, new_type)
#         new_columns.append(new_chunked_array)
#     # Create a new table with the updated schema and data
#     new_table = pa.Table.from_arrays(new_columns, schema=new_schema)
#     return new_table

save_dir = 'C:/Users/lllang/Desktop/Current_Projects/ParquetDB/data/raw/ParquetDB_Dev'

temp_dir = tempfile.mkdtemp()
db = ParquetDatasetDB(dataset_name='dev', dir=save_dir, n_cores=1)
# db.drop_table('dev')



data=[{'field1':{
            'field-11': {'field-21': {}, 'field-22': '5'} , 
            'field-12': {}}, 
        'field2':1}
    ]
struct_type = pa.struct({'x': pa.int32(), 'y': pa.string()})

field_names= [field.name for field in struct_type]
# print(struct_type.names)
print(field_names)
table=pa.Table.from_pylist(data)


schema=table.schema
# def find_struct_fields(schema, find_empty_structs=False):
#     struct_fields=[]
#     empty_fields=[]
#     for field in schema:
#         if pa.types.is_struct(field.type):
#             print(field.name, field.type)
#             print(field)
            
#             flatten_field = field.flatten()
            
#             if len(flatten_field)==0:
#                 empty_fields.append(field)
#             struct_fields.append(field)
            
#             nested_struct_fields,nested_empty_fields=find_struct_fields(flatten_field)
#             if nested_struct_fields:
#                 struct_fields.extend(nested_struct_fields)
#             if nested_empty_fields:
#                 empty_fields.extend(nested_empty_fields)

#     return struct_fields, empty_fields

def find_struct_fields(schema):
    struct_fields=[]
    for field in schema:
        if pa.types.is_struct(field.type):
            struct_fields.append(field)
            flatten_field = field.flatten()
            nested_struct_fields=find_struct_fields(flatten_field)
            if nested_struct_fields:
                struct_fields.extend(nested_struct_fields)

    return struct_fields

def find_empty_structs(schema):
    empty_fields=[]
    struct_fields=find_struct_fields(schema)
    for field in struct_fields:
        if len(field.flatten())==0:
            empty_fields.append(field)
    return empty_fields

def add_dummy_field_to_schemas_with_empty_structs(table):
    schema=table.schema
    print(schema.field('field1.field-11'))
    empty_fields=find_empty_structs(schema)
    print(empty_fields)
    for field in empty_fields:
        field_name=field.name
        nested_field_names=field_name.split('.')
        nested_field=None
        for i,nest_filed_name in enumerate(nested_field_names):
            # print(i)
            # print(dir(field.type))
            # if i==len(nested_field_names)-1:
            #     continue
            if nested_field is None:
                nested_field=schema.field(nest_filed_name)
            else:
                nested_field=nested_field.type.field(nest_filed_name)
            # if nested_field is None:
            #     nested_field=schema.field(nest_filed_name)
            # else:
            #     nested_field=nested_field.field(nest_filed_name)
        # nested_field = 
        print(nested_field)
        print(nested_field_names)
    # return struct_fields

# for field in schema:
#     print(field.name, field.type)

# empty_fields=find_empty_structs(schema)

# empty_field=empty_fields[0]
# print(empty_field)
# nested_field_names=empty_field.name.split('.')
# field=schema.field(nested_field_names[0])
# print(field)
dummy_struct_type=pa.struct([('dummy_field', pa.int16())])
def replace_empty_structs(struct_type):
    field_list=[]
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

def replace_empty_structs_in_schema(schema):
    field_list=[]
    for field in schema:
        field_name=field.name
        field_type=field.type
        if pa.types.is_struct(field.type):
            field_type=replace_empty_structs(field.type)
        else:
            field_type=field.type
        field_list.append((field_name,field_type))
    return pa.schema(field_list)


new_schema=replace_empty_structs_in_schema(schema)


def add_null_values_for_missing_nested_fields(chunked_array, new_struct_type):

    new_struct_names= [field.name for field in struct_type]
    current_names= [field.name for field in chunked_array.type]
    # table_schema = table.schema
    # current_names= [field.name for field in table_schema.field(field_name)]

    # field_type=field.type

    field_names_missing = list(set(new_struct_names) - set(current_names))
    
    # Add missing fields with null values
    for new_field_name in field_names_missing:
        field_type = new_struct_type.field(new_field_name).type
        if pa.types.is_struct(field_type):
            add_null_values_for_missing_nested_fields(chunked_array, new_struct_type)
        # null_array = pa.nulls(table.num_rows, type=field_type)
        # table = table.append_column(new_field_name, null_array)
    
    # return field
# column_table=table.select(columns=['field1'])
# column=table.column('field1')
# print(column)
# print(type(column))
# print(column.type)



####################################################################################
data=[{'field1':{
            'field-11': {'field-21': {'field-31':1}, 'field-22': '5'} , 
            'field-12': {'field-21':1}}, 
        'field2':1},
        {'field1':{
            'field-11': {'field-21': {'field-32':1}, 'field-22': '5'} , 
            'field-12': {'field-23':1}}, 
        'field2':1},
        {'field1':{
            'field-11': {'field-21': {'field-32':1}, 'field-22': '5'} , 
            'field-12': {'field-23':1}}, 
        'field2':1}
    ]

table_1=pa.Table.from_pylist(data)


data=[{'field1':{
            'field-11': {'field-21': {'field-32':1}, 'field-22': '5'} , 
            'field-12': {'field-23':1}}, 
        'field2':1}]

table_2=pa.Table.from_pylist(data)
new_schema=merge_schemas(current_schema=table_1.schema, incoming_schema=table_2.schema)
# new_schema=table_1.schema
new_table_2=align_table(table, new_schema)
print(new_table_2.to_pandas())


# # print(select_column)
# # print(type(select_column))
# # # print(table_1.select(columns=['field1']))
# # def align_struct_chunked_arrays(chunk_array_1, chunk_array_2):
# #     field_type_1=chunk_array_1.type
# #     field_type_2=chunk_array_2.type

# #     field_names_1=[field.name for field in field_type_1]
# #     field_names_2=[field.name for field in field_type_2]

# #     flattened_array_1=chunk_array_1.flatten()
# #     flattened_array_2=chunk_array_2.flatten()
    
# #     missing_field_name=list(set(field_names_1) - set(field_names_2))
# #     for missing_field in missing_field_name:
#     # for field in struct_type:
#     # print(combined_chunked_array)
#     # for field in struct_type:
#     #     field_name=field.name
#     #     # Handles the determination of the field type
#     #     if pa.types.is_struct(field.type):
#     #         # Handles empty structs.
#     #         if len(field.flatten())==0:
#     #             field_type=dummy_struct_type
#     #         # Handles nested structs.
#     #         else:
#     #             field_type=replace_empty_structs(field.type)
#     #     else:
#     #         field_type=field.type



# new_schema=table_1.schema
# new_type=merge_schemas(current_schema=table_1.column('field1').type, 
#                          incoming_schema=table_2.column('field1').type)
# print(new_type)


# def align_struct_fields(original_array: pa.ChunkedArray, new_type: pa.StructType) -> pa.ChunkedArray:

#     # Combine chunks if necessary
#     original_array = pa.concat_arrays(original_array.chunks) if isinstance(original_array, pa.ChunkedArray) else original_array
    
#     original_type = original_array.type
#     original_fields_dict = {field.name: i for i, field in enumerate(original_type)}
    
#     # Build a new struct with all the required fields
#     new_arrays = []
#     for field in new_type:
#         if field.name in original_fields_dict:
#             # If the field exists, keep the original field
#             new_arrays.append(original_array.field(original_fields_dict[field.name]))
#         else:
#             # Otherwise, fill with nulls
#             null_array = pa.nulls(len(original_array), field.type)
#             new_arrays.append(null_array)
    
#     return pa.StructArray.from_arrays(new_arrays, fields=new_type)

# # align_struct_fields(original_array=table_1.column('field1'), new_type=new_type)

# print(table_2.column('field1'))

# # new_schema
# # align_struct_columns(column_table=table_1.select('field1'), 
# #                     chunk_array_2=table_2.column('field1'))

# # new_
# chunk_array_1=table_1.column('field1')
# print(type(chunk_array_1))

# print(type(chunk_array_1[0]))
# print(chunk_array_1[0])

# # print(len(table_1.column('field1')))
# # print(type(table_1.column('field1')))
# # align_struct_chunked_arrays(chunk_array_1=table_1.column('field1'), 
# #                             chunk_array_2=table_2.column('field1'))

# Create pyarrow tables
# table_1 = pa.Table.from_pylist(data_1)
# table_2 = pa.Table.from_pylist(data_2)

# Extract schemas
# schema_1 = table_1.schema
# schema_2 = table_2.schema

# def align_struct_fields(original_array: pa.Array, new_type: pa.DataType) -> pa.Array:
#     """
#     Aligns the fields of an array to match the new schema, filling missing fields with null values.
#     Handles nested struct types recursively.

#     Args:
#         original_array (pa.Array): The original array (can be a struct or any other type).
#         new_type (pa.DataType): The target type to align to.

#     Returns:
#         pa.Array: The aligned array.
#     """
#     # Combine chunks if necessary
#     if isinstance(original_array, pa.ChunkedArray):
#         original_array = pa.concat_arrays(original_array.chunks)
    
#     # If the new type is not a struct, just cast and return
#     if not isinstance(new_type, pa.StructType):
#         return original_array.cast(new_type)
    
#     original_type = original_array.type
#     original_fields_dict = {field.name: i for i, field in enumerate(original_type)}
    
#     # Build a new struct with all the required fields
#     new_arrays = []
#     for field in new_type:
#         if field.name in original_fields_dict:
#             # If the field exists, align it recursively
#             original_field = original_array.field(original_fields_dict[field.name])
#             aligned_field = align_struct_fields(original_field, field.type)
#             new_arrays.append(aligned_field)
#         else:
#             # Otherwise, fill with nulls
#             null_array = pa.nulls(len(original_array), field.type)
#             new_arrays.append(null_array)
    
#     return pa.StructArray.from_arrays(new_arrays, fields=new_type)


# aligned_array = align_struct_fields(table_1.column('field1'), new_schema.field('field1').type)

# print("Original array:")
# print(table_1.column('field1'))
# print("\nAligned array:")
# print(aligned_array.to_pandas())