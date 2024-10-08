from glob import glob
import json
import os
import pyarrow as pa
from typing import List, Tuple, Union

from parquetdb.utils import pyarrow_utils

def compare_fields(field1: pa.Field, field2: pa.Field, path: str = "") -> List[Tuple[str, str, str]]:
    differences = []
    full_path = f"{path}.{field1.name}" if path else field1.name

    if field1.type != field2.type:
        differences.append((full_path, f"{field1.type} vs {field2.type}", "Different data types"))
    elif isinstance(field1.type, pa.StructType) and isinstance(field2.type, pa.StructType):
        differences.extend(find_schema_differences(field1.type, field2.type, full_path))
    
    if field1.nullable != field2.nullable:
        differences.append((full_path, f"nullable: {field1.nullable} vs {field2.nullable}", "Different nullable property"))

    return differences

def find_schema_differences(schema1: Union[pa.Schema, pa.StructType], 
                            schema2: Union[pa.Schema, pa.StructType], 
                            path: str = "") -> List[Tuple[str, str, str]]:
    differences = []

    fields1 = {field.name: field for field in schema1}
    fields2 = {field.name: field for field in schema2}

    for name, field in fields1.items():
        full_path = f"{path}.{name}" if path else name
        if name not in fields2:
            differences.append((full_path, str(field.type), "Field only in first schema"))
        else:
            differences.extend(compare_fields(field, fields2[name], path))

    for name, field in fields2.items():
        full_path = f"{path}.{name}" if path else name
        if name not in fields1:
            differences.append((full_path, str(field.type), "Field only in second schema"))

    return differences

# Example usage
if __name__ == "__main__":
    schema1 = pa.schema([
        ('id', pa.int64()),
        ('name', pa.string()),
        ('age', pa.int32()),
        ('score', pa.float64()),
        ('address', pa.struct([
            ('street', pa.string()),
            ('city', pa.string()),
            ('country', pa.string())
        ])),
        ('grades', pa.list_(pa.int32()))
    ])

    schema2 = pa.schema([
        ('id', pa.int64()),
        ('name', pa.string()),
        ('age', pa.int32()),
        ('grade', pa.string()),
        ('score', pa.float32()),
        ('address', pa.struct([
            ('street', pa.string()),
            ('city', pa.string()),
            ('zip', pa.string())
        ])),
        ('grades', pa.list_(pa.float32()))
    ])
    
    json_dir=os.path.join('data','external','alexandria','uncompressed')
    files=glob(os.path.join(json_dir,'*.json'))
    
    with open(files[0], 'r') as f:
        data_1 = json.load(f)
    with open(files[1], 'r') as f:
        data_2 = json.load(f)
    
    table_1=pa.Table.from_pylist(data_1['entries'])
    table_2=pa.Table.from_pylist(data_2['entries'])

    schema1=table_1.schema
    schema2=table_2.schema
    
    schema1=pyarrow_utils.merge_schemas(schema1, schema2)
    differences = find_schema_differences(schema1, schema2)
    
    if differences:
        print("Differences found:")
        for diff in differences:
            print(f"Field: {diff[0]}, Details: {diff[1]}, Reason: {diff[2]}")
    else:
        print("No differences found between the schemas.")
