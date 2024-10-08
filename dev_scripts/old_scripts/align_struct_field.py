from typing import List, Callable

import pyarrow as pa
import copy

dummy_struct_type=pa.struct([('dummy_field', pa.int16())])

def replace_empty_structs(struct_type, dummy_struct_type=pa.struct([('dummy_field', pa.int16())])):
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
        print(field_list)

    return pa.struct(field_list)




def align_struct_fields(original_array: pa.Array, new_type: pa.DataType) -> pa.Array:
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
    
    original_type = original_array.type
    original_fields_dict = {field.name: i for i, field in enumerate(original_type)}
    
    # Build a new struct with all the required fields
    new_arrays = []
    for field in new_type:
        if field.name in original_fields_dict:
            # If the field exists, align it recursively
            original_field = original_array.field(original_fields_dict[field.name])
            aligned_field = align_struct_fields(original_field, field.type)
            
            # Check if the aligned field is an empty struct
            if isinstance(field.type, pa.StructType) and len(field.type) == 0:
                # Replace empty struct with dummy struct
                dummy_array = pa.array([{'dummy_field': None}] * len(original_array), type=dummy_struct_type)
                new_arrays.append(dummy_array)
            else:
                new_arrays.append(aligned_field)
        else:
            # If the field doesn't exist, fill with nulls
            if isinstance(field.type, pa.StructType) and len(field.type) == 0:
                # For empty struct fields, use the dummy struct
                dummy_array = pa.array([{'dummy_field': None}] * len(original_array), type=dummy_struct_type)
                new_arrays.append(dummy_array)
            else:
                null_array = pa.nulls(len(original_array), field.type)
                new_arrays.append(null_array)
    
    return pa.StructArray.from_arrays(new_arrays, fields=new_type)
# Example usage
def create_example_data():
    # Original data with nested structs, including an empty struct
    data = [
        {'a': 1, 'b': {'x': 10, 'y': 20}, 'c': {}},
        {'a': 2, 'b': {'x': 30, 'y': 40}, 'c': {}},
    ]
    original_array = pa.array(data)

    # New schema with additional fields and an empty struct
    new_schema = pa.struct([
        ('a', pa.int64()),
        ('b', pa.struct([
            ('x', pa.int64()),
            ('y', pa.int64()),
            ('z', pa.int64()),  # New field
        ])),
        ('c', pa.struct([])),  # Empty struct
        ('d', pa.int64()),  # New field
    ])
    new_schema=replace_empty_structs(new_schema)
    return original_array, new_schema

def replace_empty_struct_in_schema(schema, dummy_struct_type=pa.struct([('dummy_field', pa.int16())])):
    callbacks_kwargs=[{'dummy_struct_type':dummy_struct_type}]
    new_schema=iterate_over_schema_fields(schema, callabacks = [replace_empty_structs_in_field], 
                                        callbacks_kwargs=callbacks_kwargs)
    return new_schema

def replace_empty_structs_in_field(field, dummy_struct_type=pa.struct([('dummy_field', pa.int16())])):
    field_name=field.name
    if pa.types.is_struct(field.type):
        new_type=replace_empty_structs(field.type,dummy_struct_type)
    else:
        new_type=field.type
    return pa.field(field_name, new_type)



def iterate_over_schema_fields(schema, callabacks: List[Callable], callbacks_kwargs: List[dict]=None):
    if len(callabacks)==0:
        return schema

    new_fields=[]
    for field in schema:
        new_field=copy.deepcopy(field)
        for i,callback in enumerate(callabacks):
            if callbacks_kwargs:
                callback_kwargs=callbacks_kwargs[i]
            else:
                callback_kwargs={}
            new_field=callback(new_field, **callback_kwargs)

        new_fields.append(new_field)
    return pa.schema(new_fields)

def test_callback(field):
    field_name=field.name
    if pa.types.is_struct(field.type):
        new_type=replace_empty_structs(field.type)
        return pa.field(field_name, new_type)
    else:
        return field


def main():
    # original_array, new_schema = create_example_data()
    # aligned_array = align_struct_fields(original_array, new_schema)
    
    # print("Original array:")
    # print(original_array)
    # print("\nAligned array:")
    # df=aligned_array.to_pandas()

    # print(df.iloc[0]['c'])
    # print(aligned_array.to_pandas())

    # data = [
    #     { 'c': {}},
    #     { 'c': {}},
    # ]
    data = [
        {'a': 1, 'b': {'x': 10, 'y': 20}, 'c': {}},
        {'a': 2, 'b': {'x': 30, 'y': 40}, 'c': {}},
    ]
    # print(data)
    table=pa.Table.from_pylist(data)
    print(table.schema)
    new_schema=replace_empty_struct_in_schema(schema=table.schema)
    print(new_schema)

    # result=iterate_over_schema_fields(table.schema, callabacks = [replace_empty_structs_in_field])
    # print(result)

if __name__ == "__main__":
    main()