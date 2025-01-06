import dill
dill.settings['recurse'] = True
import shutil
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
ALLOWED_TYPES = (str, int, float, bool, list, dict, tuple, bytes, np.generic)

def is_python_object(x, check_bytes=False, allowed_types=ALLOWED_TYPES):
    if isinstance(x, bytes) and check_bytes:
        x = dill.loads(x)
    return (isinstance(x, object) and not isinstance(x, allowed_types))
        
def has_python_object(values, check_bytes=False):
    """Check if a pandas Series contains Python objects (excluding simple types)."""
    is_object=False
    for value in values:
        if value is not None and is_python_object(value, check_bytes=check_bytes):
            is_object=True
            break
    return is_object


def serialize_python_objects(df):
    python_object_columns=[]
    for column in df.columns:
        values=df[column].values
        if has_python_object(values):
            python_object_columns.append(column)
            new_values=[]
            for value in values:
                if value is not None and not pd.isna(value):
                    new_values.append(dill.dumps(value))
                else:
                    new_values.append(None)
            df[column]=new_values
            
    return df, python_object_columns


def dump_python_object(value):
    if value is None:
        return None
    else:
        return dill.dumps(value)
    
def load_python_object(value):
    if value is None:
        return None
    else:
        return dill.loads(value)

def is_none(value):
    return value is None

def copy_files(paths):
    src_path, target_path = paths
    shutil.copy2(src_path, target_path)

def compress_parquet(paths):
    src_path, dest_path = paths
    table = pq.read_table(src_path)
    pq.write_table(table, dest_path, compression='gzip')