import pyarrow as pa

from parquetdb.pyarrow_utils import combine_tables, merge_schemas, align_table
# Example schemas
original_schema = pa.schema([
    ("field1", pa.int32()),
    ("sites", pa.struct([
        ("subfield1", pa.float64()),
        ("subfield2", pa.string()),
    ]))
])

incoming_schema = pa.schema([
    ("field1", pa.int32()),
    ("sites", pa.struct([
        ("subfield1", pa.float64()),
        ("subfield2", pa.string()),
        ("subfield3", pa.int32())  # New field in incoming schema
    ])),
    ("new_field", pa.string())  # Completely new field
])

# # Merge schemas
# merged_schema = merge_schemas(original_schema, incoming_schema)
# print(merged_schema)


# Original and incoming tables
original_table = pa.table([
    pa.array([1, 2, 3]),
    pa.array([
        {"subfield1": 1.1, "subfield2": "a"},
        {"subfield1": 2.2, "subfield2": "b"},
        {"subfield1": 3.3, "subfield2": "c"}
    ])
], schema=original_schema)

incoming_table = pa.table([
    pa.array([4, 5]),
    pa.array([
        {"subfield1": 4.4, "subfield2": "d", "subfield3": 10},
        {"subfield1": 5.5, "subfield2": "e", "subfield3": 20}
    ]),
    pa.array(["new1", "new2"])  # Field in incoming table only
], schema=incoming_schema)

# original_schema = original_table.schema
# incoming_schema = incoming_table.schema

# Merged schema from the previous step
merged_schema = merge_schemas(original_schema, incoming_schema)

print(merged_schema)
# # Combine the tables
combined_table = combine_tables(original_table, incoming_table)

df=combined_table.to_pandas()
print(df.head())



# new_table=align_table(original_table, merged_schema)
# df=new_table.to_pandas()

# print(df.head())

