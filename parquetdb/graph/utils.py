import pandas as pd
import pyarrow as pa

ALLOWED_DATAFRAME_TYPES = [pd.DataFrame, pa.Table, pa.RecordBatch]


def get_dataframe_column_names(df):
    if isinstance(df, pd.DataFrame):
        column_names = df.columns
    elif isinstance(df, pa.Table):
        column_names = df.schema.names
    elif isinstance(df, pa.RecordBatch):
        column_names = df.schema.names
    else:
        raise ValueError(
            f"Invalid data type for dataframe validation. Must be one of: {ALLOWED_DATAFRAME_TYPES}"
        )
    return column_names
