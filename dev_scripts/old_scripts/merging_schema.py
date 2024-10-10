from glob import glob
import json
import random
from typing import List, Callable, Dict

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import os
import copy

import tempfile

from parquetdb.utils import pyarrow_utils

from parquetdb import config

config.logging_config.loggers.parquetdb.level='ERROR'
config.logging_config.loggers.timing.level='DEBUG'
config.apply()

materials_dir = os.path.join(config.data_dir, 'raw', 'materials_data')
files = glob(os.path.join(materials_dir, '*.json'))

# Load materials data from JSON files
materials_data = []
for file in files:
    with open(file, 'r') as f:
        materials_data.append(json.load(f))
##################################################################################################



def generate_similar_data(template_data, num_entries):
    def generate_value(value):
        if isinstance(value, int):
            return random.randint(value - 10, value + 10)
        elif isinstance(value, float):
            return round(random.uniform(value * 0.8, value * 1.2), 2)
        elif isinstance(value, str):
            return f"{value}_{random.randint(1, 100)}"
        elif isinstance(value, dict):
            return {k: generate_value(v) for k, v in value.items()}
        # elif isinstance(value, list):
        #     return [generate_value(item) for item in value]
        elif value is None:
            return None
        else:
            return value

    generated_data = []
    for i in range(num_entries):
        new_entry = copy.deepcopy(random.choice(template_data))
        for key, value in new_entry.items():
            if key == 'id':
                new_entry[key] = i
            else:
                new_entry[key] = generate_value(value)
        generated_data.append(new_entry)

    return generated_data

current_struct = pa.array([
        {'id': 0, 'b': {'x': 10, 'y': 20}, 'c': {}, 'e':5},
        {'id': 1, 'b': {'x': 30, 'y': 40}, 'c': {}},
    ])

current_data=current_struct.to_pylist()
current_schema=pa.schema(current_struct.type)
# current_table=pa.Table.from_struct_array(current_struct)

incoming_ids=[2,3,0]
incoming_struct = pa.array([
        {'id': 2, 'b': {'x': 10, 'y': 20}, 'c': {}},
        {'id': 3, 'b': {'x': 10, 'y': 20}, 'c': {}},
        {'id': 0, 'b': {'y': 50, 'z':{'a':1.1,'b':3.3}}, 'c': {},'d':1, 'e':6},
    ])

incoming_schema=pa.schema(incoming_struct.type)
current_struct = pa.array(current_data)

# Example usage
current_data = current_struct.to_pylist()
incoming_data = incoming_struct.to_pylist()

# Combine current and incoming data to have a more diverse template
combined_template = current_data + incoming_data

# Generate new data
new_data_count = 5  # Change this to generate more or fewer entries
current_data = generate_similar_data(combined_template, num_entries=1000000)
incoming_data = generate_similar_data(combined_template, num_entries=100000)

# data_list=[]
# for i in range(10000):
#     data = random.choices(materials_data,k=1)[0]
#     data_list.append(data)

# current_struct = pa.array(data_list)

# data1=current_struct.to_pylist()
# current_schema=pa.schema(current_struct.type)
# current_table=pa.Table.from_struct_array(current_struct)


current_schema=pa.schema(current_struct.type)

incoming_struct = pa.array(incoming_data)

incoming_schema=pa.schema(incoming_struct.type)






union_schema = pa.unify_schemas([current_schema, incoming_schema],promote_options='permissive')

current_table=pa.Table.from_pylist(current_data, schema=union_schema)
incoming_table=pa.Table.from_pylist(incoming_struct, schema=union_schema)


for x in incoming_table['id'].combine_chunks():
    print(x)

# current_table=pyarrow_utils.fill_null_nested_structs_in_table(current_table)
# incoming_table=pyarrow_utils.fill_null_nested_structs_in_table(incoming_table)

# current_table=pyarrow_utils.replace_empty_structs_in_table(current_table)
# incoming_table=pyarrow_utils.replace_empty_structs_in_table(incoming_table)



# print(f"Current table shape: {current_table.shape}")
# print(f"Incoming table shape: {incoming_table.shape}")

# print(f"\n\nCurrent schema: {current_table.schema}\n\n")
# print(f"\n\nIncoming schema: {incoming_table.schema}\n\n")


# updated_table=pyarrow_utils.update_table(current_table, incoming_table, flatten_method=True)
# def my_function():
#     updated_table=pyarrow_utils.update_table(current_table, incoming_table, flatten_method=True)
# # print(updated_table.to_pandas())


# # print('\n')
# # updated_table=pyarrow_utils.update_table(current_table, incoming_table, flatten_method=False)
# # print(updated_table.to_pandas())

# import cProfile
# import pstats
# import io
# if __name__ == "__main__":
#     profiler = cProfile.Profile()
#     profiler.enable()  # Start profiling

#     my_function()
    
#     profiler.disable()  # Stop profiling
    
#     # Create an IO stream to capture the profiler results
#     s = io.StringIO()
#     ps = pstats.Stats(profiler, stream=s)

#     # Sort by 'tottime' (Total time spent in the function excluding time spent in calls to sub-functions)
#     ps.sort_stats('tottime')

#     # Print or log the profiler data
#     ps.print_stats()

#     # Now you can log the output to a file
#     with open('data/profile_results.log', 'w') as f:
#         f.write(s.getvalue())


# def update_all_nested_structs(current_table, incoming_table, parent_name='b'):
    
#     def update_struct_field(current_array, incoming_table, field_path):
#         print("Parent name: ", parent_name)
#         print("field_path: ", field_path)
#         full_field_path=[parent_name]+field_path
#         incoming_filter = (
#             pc.field('id').isin(incoming_table['id']) & 
#             ~pc.field(*full_field_path).is_null()
#         )
#         filtered_incoming_table = incoming_table.filter(incoming_filter)
#         incoming_chunked_array = pc.struct_field(filtered_incoming_table[parent_name], field_path).combine_chunks()
        
#         mask = pc.is_in(current_table['id'], filtered_incoming_table['id'])
        
#         new_array= pc.replace_with_mask(current_array,
#                                         mask.combine_chunks(), 
#                                         incoming_chunked_array)
#         return new_array
    
#     def update_nested_field(current_parent_array, incoming_parent_array, field):
#         parent_field_name=field.name
#         parent_field_type=field.type
    
#         # print(type(current_array))
#         if isinstance(current_parent_array, pa.ChunkedArray):
#             current_parent_array=current_parent_array.combine_chunks()
#         if isinstance(incoming_parent_array, pa.ChunkedArray):
#             incoming_parent_array=incoming_parent_array.combine_chunks()
#         # print(type(current_array))
        
#         updated_field_arrays=[]
#         updated_fields=[]
#         field_names=[]

        
#         if not pa.types.is_struct(parent_field_type):
#             field_path=[parent_field_name]
#             new_array=update_struct_field(current_parent_array, incoming_parent_array, field_path)
#             return new_array

#         print(parent_field_name)
#         child_field_names=[field.name for field in current_parent_array.type]
#         for field_name in child_field_names:
#             if pa.types.is_struct(current_parent_array.type):
#                 child_field=parent_field_type.field(field_name)
#                 current_child_array = current_parent_array.field(field_name)
#                 incoming_child_array = incoming_parent_array.field(field_name)
#                 updated_field_array = update_nested_field(current_child_array, incoming_child_array, child_field)
                
#                 updated_field_arrays.append(updated_field_array)
#             else:
#                 new_array=update_struct_field(current_array, incoming_array, field_path)
#                 updated_field_arrays.append(new_array)
            
#         return pa.StructArray.from_arrays(updated_field_array, fields=child_field)
#     # child_field_names=[field.name for field in current_table.schema.field(parent_name)]
#     # Start the recursive update process
#     updated_struct = update_nested_field(current_table[parent_name], incoming_table[parent_name], current_table.schema.field(parent_name))

#     index=current_table.schema.get_field_index(parent_name)
#     field=current_table.schema.field(parent_name)
#     updated_table.set_column(index,field,updated_struct)

#     return updated_table






####################################################################################################
# Join testing
####################################################################################################
# current_table_flattened=flatten_table(current_table)
# incoming_table_flattened=flatten_table(incoming_table)

# # joined_table=current_table_flattened.join(incoming_table_flattened, keys='id', join_type="left semi")
# joined_table=current_table_flattened.join(incoming_table_flattened, keys='id', join_type="right semi")
# # joined_table=current_table_flattened.join(incoming_table_flattened, keys='id', join_type="inner", coalesce_keys=False)
# print(joined_table.to_pandas())
# print(joined_table.to_pandas())
# current_table=pyarrow_utils.replace_empty_structs_in_table(current_table)
# incoming_table=pyarrow_utils.replace_empty_structs_in_table(incoming_table)
# # incoming_table=pa.Table.from_struct_array(incoming_struct)

    