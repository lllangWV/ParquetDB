
from typing import List, Callable, Dict

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import os
import copy

import tempfile

from parquetdb.utils import pyarrow_utils
struct1 = pa.array([
        {'a': 1, 'b': {'x': 10, 'y': 20}, 'c': {}},
        {'a': 2, 'b': {'x': 30, 'y': 40}, 'c': {}},
    ])

data1=struct1.to_pylist()
schema1=pa.schema(struct1.type)
# table1=pa.Table.from_struct_array(struct1)


struct2 = pa.array([
        {'a': 1, 'b': {'x': 10, 'y': 20}, 'c': {}},
        {'a': 1, 'b': {'x': 10, 'y': 20}, 'c': {}},
        {'a': 2, 'b': {'x': 30, 'y': 40, 'z':{'a':1,'b':3}}, 'c': {},'d':1},
    ])
schema2=pa.schema(struct2.type)




union_schema = pa.unify_schemas([schema1, schema2],promote_options='permissive')


table1=pa.Table.from_pylist(data1, schema=union_schema)
table2=pa.Table.from_pylist(struct2, schema=union_schema)


table1=pyarrow_utils.replace_empty_structs_in_table(table1)
table2=pyarrow_utils.replace_empty_structs_in_table(table2)
# table2=pa.Table.from_struct_array(struct2)


table=pa.concat_tables([table1,table2])

# pq.write_table(table, os.path.join(save_dir,'concat_table.parquet'))

print(table.to_pandas(),'\n')


pq.write_table(table1, 'table1.parquet')
pq.write_table(table, 'concat_table.parquet')

union_schema=pa.schema([
                        pa.field('a', pa.int64()),
                        pa.field('b', pa.struct([
                            pa.field('x', pa.int64()),
                            pa.field('y', pa.int64()),
                            pa.field('z', pa.struct([
                                pa.field('a', pa.int64()),
                                pa.field('b', pa.int64())
                            ])),
                            pa.field('w', pa.int64())
                        ])),
                        pa.field('c', pa.struct([])),
                        pa.field('d', pa.int64()),
                        ])


table1=pq.read_table('table1.parquet')

table1=pa.Table.from_pylist(table1.to_pylist(), schema=union_schema)

print(table1.to_pandas())

# print(struct.type,'\n')
# schema=pa.schema(struct.type)
# print(schema, '\n')
# print(table1.schema,'\n')

# print(table2.schema,'\n')
# # df=table2.to_pandas()

# print(df)

# new_struct=merge_structs(pa.struct(table1.schema),pa.struct(table2.schema))
# print(new_struct,'\n')

# union_schema = pa.unify_schemas([table1.schema, table2.schema],promote_options='default') # Can also be Permissive which takes the type of the greatest common denominator
# union_schema = pa.unify_schemas([table1.schema, table2.schema],promote_options='permissive')
# print(union_schema, '\n')



# table1=table1.cast(union_schema)

# print(table1.schema,'\n')
# print(union_schema,'\n')


    