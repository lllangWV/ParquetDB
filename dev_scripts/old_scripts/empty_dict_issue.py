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
import pyarrow.compute as pc

from parquetdb import ParquetDBManager, ParquetDB
from parquetdb.utils.pyarrow_utils import  merge_schemas, merge_structs

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





# def replace_empty_structs(struct_type, dummy_field=pa.field('dummy_field', pa.int16())):
#     dummy_struct_type=pa.struct([dummy_field])
#     # If the struct is empty, return the dummy struct
#     if len(struct_type)==0:
#         return dummy_struct_type

#     field_list=[]
#     # Iterate over the fields in the struct
#     for field in struct_type:
#         field_name=field.name
#         # Handles the determination of the field type
#         if pa.types.is_struct(field.type):
#             # Handles empty structs.
#             if len(field.flatten())==0:
#                 field_type=dummy_struct_type
#             # Handles nested structs.
#             else:
#                 field_type=replace_empty_structs(field.type)
#         else:
#             field_type=field.type

#         field_list.append((field_name,field_type))

#     return pa.struct(field_list)

# def replace_empty_structs_in_struct(column_array: pa.Array, dummy_field=pa.field('dummy_field', pa.int16())) -> pa.Array:
#     dummy_struct_type=pa.struct([dummy_field])

#     # Combine chunks if necessary
#     if isinstance(column_array, pa.ChunkedArray):
#         column_array = column_array.combine_chunks()

#     # Detecting if array is a struct type
#     original_type = column_array.type
#     if pa.types.is_struct(original_type):
#         # Adding dummy field to the struct type
#         new_type = replace_empty_structs(original_type)
#     else:
#         # If the array is not a struct type, return the original array
#         return column_array

#     original_fields_dict = {field.name: i for i, field in enumerate(original_type)}

#     # Build a new struct with all the required fields
#     new_arrays=[]
#     for field in new_type:
#         if field.name in original_fields_dict:
#             logger.debug("Adding values to a existing field")
#             # Recursively generate the new array for the field
#             field_array = column_array.field(original_fields_dict[field.name])
#             new_field_array = replace_empty_structs_in_struct(field_array)
#             new_arrays.append(new_field_array)
#         else:
#             logger.debug("Adding null values to a previously non-existing field")
#             null_array = pa.nulls(len(column_array), field.type)
#             new_arrays.append(null_array)
#     return pa.StructArray.from_arrays(new_arrays, fields=new_type)


def replace_empty_structs(column_array: pa.Array, dummy_field=pa.field('dummy_field', pa.int16())):
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


current_data = [
        {'b': {'x': 10, 'y': 20, 'z':{}}, 'c': {}},
        {'b': {'x': 30, 'y': 40, 'z':{}}, 'c': {}},
    ]



current_table=pa.Table.from_pylist(current_data)


current_table=replace_empty_structs_in_table(current_table)


print(current_table.to_pandas())


















# struct_type = pa.struct({'x': pa.int32(), 'y': pa.string()})

# field_names= [field.name for field in struct_type]
# # print(struct_type.names)
# print(field_names)
# table=pa.Table.from_pylist(data)


# schema=table.schema
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
# #             if nested_empty_fields:
# #                 empty_fields.extend(nested_empty_fields)

# #     return struct_fields, empty_fields

# def find_struct_fields(schema):
#     struct_fields=[]
#     for field in schema:
#         if pa.types.is_struct(field.type):
#             struct_fields.append(field)
#             flatten_field = field.flatten()
#             nested_struct_fields=find_struct_fields(flatten_field)
#             if nested_struct_fields:
#                 struct_fields.extend(nested_struct_fields)

#     return struct_fields

# def find_empty_structs(schema):
#     empty_fields=[]
#     struct_fields=find_struct_fields(schema)
#     for field in struct_fields:
#         if len(field.flatten())==0:
#             empty_fields.append(field)
#     return empty_fields

# def add_dummy_field_to_schemas_with_empty_structs(table):
#     schema=table.schema
#     print(schema.field('field1.field-11'))
#     empty_fields=find_empty_structs(schema)
#     print(empty_fields)
#     for field in empty_fields:
#         field_name=field.name
#         nested_field_names=field_name.split('.')
#         nested_field=None
#         for i,nest_filed_name in enumerate(nested_field_names):
#             # print(i)
#             # print(dir(field.type))
#             # if i==len(nested_field_names)-1:
#             #     continue
#             if nested_field is None:
#                 nested_field=schema.field(nest_filed_name)
#             else:
#                 nested_field=nested_field.type.field(nest_filed_name)
#             # if nested_field is None:
#             #     nested_field=schema.field(nest_filed_name)
#             # else:
#             #     nested_field=nested_field.field(nest_filed_name)
#         # nested_field = 
#         print(nested_field)
#         print(nested_field_names)
#     # return struct_fields

# # for field in schema:
# #     print(field.name, field.type)

# # empty_fields=find_empty_structs(schema)

# # empty_field=empty_fields[0]
# # print(empty_field)
# # nested_field_names=empty_field.name.split('.')
# # field=schema.field(nested_field_names[0])
# # print(field)
# dummy_struct_type=pa.struct([('dummy_field', pa.int16())])
# def replace_empty_structs(struct_type):
#     field_list=[]
#     for field in struct_type:
#         field_name=field.name
#         # Handles the determination of the field type
#         if pa.types.is_struct(field.type):
#             # Handles empty structs.
#             if len(field.flatten())==0:
#                 field_type=dummy_struct_type
#             # Handles nested structs.
#             else:
#                 field_type=replace_empty_structs(field.type)
#         else:
#             field_type=field.type

#         field_list.append((field_name,field_type))
#     return pa.struct(field_list)

# def replace_empty_structs_in_schema(schema):
#     field_list=[]
#     for field in schema:
#         field_name=field.name
#         field_type=field.type
#         if pa.types.is_struct(field.type):
#             field_type=replace_empty_structs(field.type)
#         else:
#             field_type=field.type
#         field_list.append((field_name,field_type))
#     return pa.schema(field_list)


# new_schema=replace_empty_structs_in_schema(schema)


# def add_null_values_for_missing_nested_fields(chunked_array, new_struct_type):

#     new_struct_names= [field.name for field in struct_type]
#     current_names= [field.name for field in chunked_array.type]
#     # table_schema = table.schema
#     # current_names= [field.name for field in table_schema.field(field_name)]

#     # field_type=field.type

#     field_names_missing = list(set(new_struct_names) - set(current_names))
    
#     # Add missing fields with null values
#     for new_field_name in field_names_missing:
#         field_type = new_struct_type.field(new_field_name).type
#         if pa.types.is_struct(field_type):
#             add_null_values_for_missing_nested_fields(chunked_array, new_struct_type)
#         # null_array = pa.nulls(table.num_rows, type=field_type)
#         # table = table.append_column(new_field_name, null_array)
    
    # return field
# column_table=table.select(columns=['field1'])
# column=table.column('field1')
# print(column)
# print(type(column))
# print(column.type)



####################################################################################
# data=[{'field1':{
#             'field-11': {'field-21': {'field-31':1}, 'field-22': '5'} , 
#             'field-12': {'field-21':1}}, 
#         'field2':1},
#         {'field1':{
#             'field-11': {'field-21': {'field-32':1}, 'field-22': '5'} , 
#             'field-12': {'field-23':1}}, 
#         'field2':1},
#         {'field1':{
#             'field-11': {'field-21': {'field-32':1}, 'field-22': '5'} , 
#             'field-12': {'field-23':1}}, 
#         'field2':1}
#     ]

# table_1=pa.Table.from_pylist(data)


# data=[{'field1':{
#             'field-11': {'field-21': {'field-32':1}, 'field-22': '5'} , 
#             'field-12': {'field-23':1}}, 
#         'field2':1}]

# table_2=pa.Table.from_pylist(data)
# new_schema=merge_schemas(current_schema=table_1.schema, incoming_schema=table_2.schema)
# # new_schema=table_1.schema
# new_table_2=align_table(table, new_schema)
# print(new_table_2.to_pandas())


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