import logging
import os
from functools import wraps
from pathlib import Path
from typing import Dict, List, Union

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

from parquetdb import ParquetDB
from parquetdb.core.parquetdb import LoadConfig, NormalizeConfig
from parquetdb.graph.utils import get_dataframe_column_names

logger = logging.getLogger(__name__)

REQUIRED_EDGE_COLUMNS_FIELDS = set(
    ["source_id", "source_type", "target_id", "target_type", "edge_type"]
)


def validate_edge_dataframe(df):
    column_names = get_dataframe_column_names(df)
    fields = set(column_names)
    missing_fields = REQUIRED_EDGE_COLUMNS_FIELDS - fields
    if missing_fields:
        raise ValueError(
            f"Edge dataframe is missing required fields: {missing_fields}. Edge dataframe must contain the following columns: {REQUIRED_EDGE_COLUMNS_FIELDS}"
        )
    return df


def edge_generator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Perform pre-execution checks
        logger.debug(f"Executing {func.__name__} with args: {args}, kwargs: {kwargs}")
        df = func(*args, **kwargs)
        validate_edge_dataframe(df)
        return df

    wrapper.__name__ = func.__name__
    wrapper.__doc__ = func.__doc__
    return wrapper


class EdgeStore(ParquetDB):
    """
    A wrapper around ParquetDB specifically for storing edge features
    of a given edge type.
    """

    required_fields = REQUIRED_EDGE_COLUMNS_FIELDS
    edge_metadata_keys = ["class", "class_module"]

    def __init__(
        self,
        storage_path: Union[str, Path],
        setup_kwargs: dict = None,
        verbose: int = 1,
    ):
        """
        Parameters
        ----------
        storage_path : str
            The path where ParquetDB files for this edge type are stored.
        """
        storage_path = (
            Path(storage_path) if isinstance(storage_path, str) else storage_path
        )

        super().__init__(
            db_path=storage_path,
            initial_fields=[
                pa.field("source_id", pa.int64()),
                pa.field("source_type", pa.string()),
                pa.field("target_id", pa.int64()),
                pa.field("target_type", pa.string()),
                pa.field("edge_type", pa.string()),
            ],
            verbose=verbose,
        )

        self._initialize_metadata()
        self._initialize_field_metadata()

        logger.debug(f"Initialized EdgeStore at {storage_path}")
        if self.is_empty():
            setup_kwargs = {} if setup_kwargs is None else setup_kwargs
            self._setup(**setup_kwargs)

    def __repr__(self):
        return self.summary(show_column_names=False)

    @property
    def storage_path(self):
        return self._db_path

    @storage_path.setter
    def storage_path(self, value):
        self._db_path = value
        self.edge_type = value.name if isinstance(value, Path) else value

    @property
    def edge_type(self):
        return (
            self.storage_path.name
            if isinstance(self.storage_path, Path)
            else self.storage_path
        )

    @edge_type.setter
    def edge_type(self, value):
        self._edge_type = value

    @property
    def n_edges(self):
        return self.read_edges(columns=["id"]).num_rows

    @property
    def n_features(self):
        return len(self.get_schema().names)

    @property
    def columns(self):
        return self.get_schema().names

    def summary(self, show_column_names: bool = False):
        fields_metadata = self.get_field_metadata()
        metadata = self.get_metadata()
        # Header section
        tmp_str = f"{'=' * 60}\n"
        tmp_str += f"EDGE STORE SUMMARY\n"
        tmp_str += f"{'=' * 60}\n"
        tmp_str += f"Edge type: {self.edge_type}\n"
        tmp_str += f"• Number of edges: {self.n_edges}\n"
        tmp_str += f"• Number of features: {self.n_features}\n"
        tmp_str += f"Storage path: {os.path.abspath(self.storage_path)}\n\n"

        # Metadata section
        tmp_str += f"\n{'#' * 60}\n"
        tmp_str += f"METADATA\n"
        tmp_str += f"{'#' * 60}\n"
        for key, value in metadata.items():
            tmp_str += f"• {key}: {value}\n"

        # Node details
        tmp_str += f"\n{'#' * 60}\n"
        tmp_str += f"EDGE DETAILS\n"
        tmp_str += f"{'#' * 60}\n"
        if show_column_names:
            tmp_str += f"• Columns:\n"
            for col in self.columns:
                tmp_str += f"    - {col}\n"

                if fields_metadata[col]:
                    tmp_str += f"       - Field metadata\n"
                    for key, value in fields_metadata[col].items():
                        tmp_str += f"           - {key}: {value}\n"

        return tmp_str

    def _setup(self, **kwargs):
        data = self.setup(**kwargs)
        if data is not None:
            self.create_edges(data=data)
            self.set_metadata(kwargs)

    def setup(self, **kwargs):
        return None

    def _initialize_metadata(self, **kwargs):
        pass
        # metadata = self.get_metadata()
        # update_metadata = False
        # for key in self.edge_metadata_keys:
        #     if key not in metadata:
        #         update_metadata = update_metadata or key not in metadata

        # if update_metadata:
        #     self.set_metadata(
        #         {
        #             "class": f"{self.__class__.__name__}",
        #             "class_module": f"{self.__class__.__module__}",
        #         }
        #     )

    def _initialize_field_metadata(self, **kwargs):
        pass

    def create_edges(
        self,
        data: Union[List[dict], dict, pd.DataFrame],
        schema: pa.Schema = None,
        metadata: dict = None,
        fields_metadata: dict = None,
        treat_fields_as_ragged: List[str] = None,
        convert_to_fixed_shape: bool = True,
        normalize_dataset: bool = False,
        normalize_config: dict = NormalizeConfig(),
    ):
        """
        Adds new data to the database.

        Parameters
        ----------
        data : dict, list of dict, or pandas.DataFrame
            The data to be added to the database.
        schema : pyarrow.Schema, optional
            The schema for the incoming data.
        metadata : dict, optional
            Metadata to be attached to the table.
        fields_metadata : dict, optional
            A dictionary containing the metadata to be set for the fields.
        normalize_dataset : bool, optional
            If True, the dataset will be normalized after the data is added (default is True).
        treat_fields_as_ragged : list of str, optional
            A list of fields to treat as ragged arrays.
        convert_to_fixed_shape : bool, optional
            If True, the ragged arrays will be converted to fixed shape arrays.
        normalize_config : NormalizeConfig, optional
            Configuration for the normalization process, optimizing performance by managing row distribution and file structure.
        Examples
        --------
        >>> db.create_nodes(data=my_data, schema=my_schema, metadata={'source': 'api'}, normalize_dataset=True)
        """
        create_kwargs = dict(
            data=data,
            schema=schema,
            metadata=metadata,
            fields_metadata=fields_metadata,
            treat_fields_as_ragged=treat_fields_as_ragged,
            convert_to_fixed_shape=convert_to_fixed_shape,
            normalize_dataset=normalize_dataset,
            normalize_config=normalize_config,
        )
        self.create(**create_kwargs)

    def create(self, **kwargs):
        logger.debug(f"Creating edges")

        if not self.validate_edges(kwargs["data"]):
            logger.error("Edge data validation failed - missing required fields")
            raise ValueError(
                "Edge data is missing required fields. Must include: "
                + ", ".join(EdgeStore.required_fields)
            )

        super().create(**kwargs)

        logger.info(f"Successfully created edges")

    def read_edges(
        self,
        ids: List[int] = None,
        columns: List[str] = None,
        filters: List[pc.Expression] = None,
        load_format: str = "table",
        batch_size: int = None,
        include_cols: bool = True,
        rebuild_nested_struct: bool = False,
        rebuild_nested_from_scratch: bool = False,
        load_config: LoadConfig = LoadConfig(),
        normalize_config: NormalizeConfig = NormalizeConfig(),
    ):
        """
        Reads data from the database.

        Parameters
        ----------

        ids : list of int, optional
            A list of IDs to read. If None, all data is read (default is None).
        columns : list of str, optional
            The columns to include in the output. If None, all columns are included (default is None).
        filters : list of pyarrow.compute.Expression, optional
            Filters to apply to the data (default is None).
        load_format : str, optional
            The format of the returned data: 'table' or 'batches' (default is 'table').
        batch_size : int, optional
            The batch size to use for loading data in batches. If None, data is loaded as a whole (default is None).
        include_cols : bool, optional
            If True, includes only the specified columns. If False, excludes the specified columns (default is True).
        rebuild_nested_struct : bool, optional
            If True, rebuilds the nested structure (default is False).
        rebuild_nested_from_scratch : bool, optional
            If True, rebuilds the nested structure from scratch (default is False).
        load_config : LoadConfig, optional
            Configuration for loading data, optimizing performance by managing memory usage.
        normalize_config : NormalizeConfig, optional
            Configuration for the normalization process, optimizing performance by managing row distribution and file structure.

        Returns
        -------
        pa.Table, generator, or dataset
            The data read from the database. The output can be in table format or as a batch generator.

        Examples
        --------
        >>> data = db.read_edges(ids=[1, 2, 3], columns=['name', 'age'], filters=[pc.field('age') > 18])
        """
        logger.debug(f"Reading edges with ids: {ids}, columns: {columns}")

        read_kwargs = dict(
            ids=ids,
            columns=columns,
            filters=filters,
            load_format=load_format,
            batch_size=batch_size,
            include_cols=include_cols,
            rebuild_nested_struct=rebuild_nested_struct,
            rebuild_nested_from_scratch=rebuild_nested_from_scratch,
            load_config=load_config,
            normalize_config=normalize_config,
        )
        return self.read(**read_kwargs)

    def update(self, **kwargs):
        logger.debug(f"Updating edges")
        # if not self.validate_edges(kwargs["data"]):
        #     logger.error("Edge data validation failed - missing required fields")
        #     raise ValueError(
        #         "Edge data is missing required fields. Must include: "
        #         + ", ".join(EdgeStore.required_fields)
        #     )

        super().update(**kwargs)
        logger.info("Successfully updated edges")

    def update_edges(
        self,
        data: Union[List[dict], dict, pd.DataFrame],
        schema: pa.Schema = None,
        metadata: dict = None,
        fields_metadata: dict = None,
        update_keys: Union[List[str], str] = "id",
        treat_fields_as_ragged=None,
        convert_to_fixed_shape: bool = True,
        normalize_config: NormalizeConfig = NormalizeConfig(),
    ):
        """
        Updates existing records in the database.

        Parameters
        ----------
        data : dict, list of dicts, or pandas.DataFrame
            The data to be updated in the database. Each record must contain an 'id' key
            corresponding to the record to be updated.
        schema : pyarrow.Schema, optional
            The schema for the data being added. If not provided, it will be inferred.
        metadata : dict, optional
            Additional metadata to store alongside the data.
        fields_metadata : dict, optional
            A dictionary containing the metadata to be set for the fields.
        update_keys : list of str or str, optional
            The keys to use for updating the data. If a list, the data must contain a value for each key.
        treat_fields_as_ragged : list of str, optional
            A list of fields to treat as ragged arrays.
        convert_to_fixed_shape : bool, optional
            If True, the ragged arrays will be converted to fixed shape arrays.
        normalize_config : NormalizeConfig, optional
            Configuration for the normalization process, optimizing performance by managing row distribution and file structure.

        Examples
        --------
        >>> db.update(data=[{'id': 1, 'name': 'John', 'age': 30}, {'id': 2, 'name': 'Jane', 'age': 25}])
        """
        logger.debug(f"Updating edges")

        # if not self.validate_edges(data):
        #     logger.error("Edge data validation failed - missing required fields")
        #     raise ValueError(
        #         "Edge data is missing required fields. Must include: "
        #         + ", ".join(EdgeStore.required_fields)
        #     )

        update_kwargs = dict(
            data=data,
            schema=schema,
            metadata=metadata,
            update_keys=update_keys,
            fields_metadata=fields_metadata,
            treat_fields_as_ragged=treat_fields_as_ragged,
            convert_to_fixed_shape=convert_to_fixed_shape,
            normalize_config=normalize_config,
        )
        self.update(**update_kwargs)
        logger.info("Successfully updated edges")

    def delete_edges(
        self,
        ids: List[int] = None,
        columns: List[str] = None,
        normalize_config: NormalizeConfig = NormalizeConfig(),
    ):
        """
        Deletes records from the database.

        Parameters
        ----------
        ids : list of int
            A list of record IDs to delete from the database.
        columns : list of str, optional
            A list of column names to delete from the dataset. If not provided, it will be inferred from the existing data (default: None).
        normalize_config : NormalizeConfig, optional
            Configuration for the normalization process, optimizing performance by managing row distribution and file structure.

        Returns
        -------
        None

        Examples
        --------
        >>> db.delete(ids=[1, 2, 3])
        """
        logger.debug(f"Deleting edges with ids: {ids}, columns: {columns}")
        self.delete(ids=ids, columns=columns, normalize_config=normalize_config)
        logger.info(f"Successfully deleted edges")

    def normalize_edges(self, normalize_config: NormalizeConfig = NormalizeConfig()):
        """
        Triggers file restructuring and compaction to optimize edge storage.
        """
        logger.info("Starting edge store normalization")
        self.normalize(normalize_config=normalize_config)
        logger.info("Completed edge store normalization")

    def validate_edges(
        self, data: Union[List[dict], dict, pd.DataFrame, pa.Table, pa.RecordBatch]
    ):
        """
        Validates the edges to ensure they contain the required fields.
        """
        logger.debug("Validating edge data")

        data = ParquetDB.construct_table(data)

        if isinstance(data, pa.Table) or isinstance(data, pa.RecordBatch):
            fields = data.schema.names
        else:
            logger.error(f"Invalid data type for edge validation: {type(data)}")
            raise ValueError("Invalid data type for edge validation")

        is_valid = True
        missing_fields = []
        for required_field in EdgeStore.required_fields:
            if required_field not in fields:
                is_valid = False
                missing_fields.append(required_field)

        if not is_valid:
            logger.warning(f"Edge validation failed. Missing fields: {missing_fields}")
        else:
            logger.debug("Edge validation successful")

        return is_valid
