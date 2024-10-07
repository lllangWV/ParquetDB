from typing import List, Callable, Dict

import pyarrow as pa
import copy


class SchemaModifier:
    def __init__(self):
        self.callbacks = []
        self.callbacks_kwargs = []

    def add_callback(self, callback: Callable, callback_kwargs: Dict = None):
        """Add a callback function along with its keyword arguments."""
        self.callbacks.append(callback)
        if callback_kwargs is None:
            callback_kwargs = {}
        self.callbacks_kwargs.append(callback_kwargs)

    def modify_schema(self, schema: pa.Schema):
        """Iterate over schema fields and apply the callbacks."""
        return self._iterate_over_schema_fields(schema, self.callbacks, self.callbacks_kwargs)

    def _iterate_over_schema_fields(self, schema: pa.Schema, callbacks: List[Callable], callbacks_kwargs: List[Dict] = None):
        """Internal function to iterate over schema fields and apply each callback."""
        if len(callbacks) == 0:
            return schema

        new_fields = []
        for field in schema:
            new_field = copy.deepcopy(field)
            for i, callback in enumerate(callbacks):
                if callbacks_kwargs:
                    callback_kwargs = callbacks_kwargs[i]
                else:
                    callback_kwargs = {}
                new_field = callback(new_field, **callback_kwargs)

            new_fields.append(new_field)
        return pa.schema(new_fields)

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

    return pa.struct(field_list)


def replace_empty_structs_in_field(field, dummy_struct_type=pa.struct([('dummy_field', pa.int16())])):
    if pa.types.is_struct(field.type):
        field_name=field.name
        if pa.types.is_struct(field.type):
            new_type=replace_empty_structs(field.type,dummy_struct_type)
        else:
            new_type=field.type

    else:
        field_name=field.name
        new_type=field.type
    
    return pa.field(field_name, new_type)


data = [
        {'a': 1, 'b': {'x': 10, 'y': 20}, 'c': {}},
        {'a': 2, 'b': {'x': 30, 'y': 40}, 'c': {}},
    ]
# print(data)
table=pa.Table.from_pylist(data)

if __name__ == "__main__":
    # Initialize the SchemaModifier
    modifier = SchemaModifier()

    # Add the callback with arguments
    modifier.add_callback(replace_empty_structs_in_field, {'dummy_struct_type': pa.struct([('dummy_field', pa.int16())])})

    # Define a schema and modify it
    original_schema = pa.schema([pa.field('example_field', pa.struct([]))])
    new_schema = modifier.modify_schema(original_schema)

    print(new_schema)