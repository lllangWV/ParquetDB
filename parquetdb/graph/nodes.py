import logging
import os
from functools import cached_property, wraps
from pathlib import Path
from typing import List, Union

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

from parquetdb import ParquetDB
from parquetdb.core.parquetdb import LoadConfig, NormalizeConfig
from parquetdb.graph.generator_store import validate_generator_inputs
from parquetdb.graph.utils import get_dataframe_column_names

logger = logging.getLogger(__name__)


REQUIRED_NODE_COLUMNS_FIELDS = set()


def validate_node_dataframe(df):
    column_names = get_dataframe_column_names(df)
    fields = set(column_names)
    missing_fields = REQUIRED_NODE_COLUMNS_FIELDS - fields
    if missing_fields:
        raise ValueError(
            f"Node dataframe is missing required fields: {missing_fields}. Node dataframe must contain the following columns: {REQUIRED_NODE_COLUMNS_FIELDS}"
        )
    return df


def node_generator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Perform pre-execution checks
        logger.debug(f"Executing {func.__name__} with args: {args}, kwargs: {kwargs}")
        validate_generator_inputs(args)

        df = func(*args, **kwargs)
        validate_node_dataframe(df)
        return df

    return wrapper


class NodeStore(ParquetDB):
    """
    A wrapper around ParquetDB specifically for storing node features
    of a given node type.
    """

    node_metadata_keys = ["node_type", "name_column"]

    def __init__(
        self,
        storage_path: Union[str, Path],
        initialize_kwargs: dict = None,
        verbose: int = 1,
    ):
        """
        Parameters
        ----------
        storage_path : str
            The path where ParquetDB files for this node type are stored.
        """
        storage_path = Path(storage_path)
        self._node_type = storage_path.name

        initialize_kwargs = {} if initialize_kwargs is None else initialize_kwargs

        super().__init__(db_path=storage_path, verbose=verbose)

        metadata = self.get_metadata()

        update_metadata = False
        for key in self.node_metadata_keys:
            if key not in metadata:
                update_metadata = update_metadata or key not in metadata
        if update_metadata:
            self.set_metadata(
                {
                    "node_type": self._node_type,
                    "name_column": "id",
                }
            )

        if self.is_empty():
            self._initialize(**initialize_kwargs)

        logger.debug(f"Initialized NodeStore at {storage_path}")

    def __repr__(self):
        return self.summary(show_column_names=False)

    @property
    def storage_path(self):
        return self._db_path

    @storage_path.setter
    def storage_path(self, value):
        self._db_path = value
        self._node_type = value.name if isinstance(value, Path) else value

    def _initialize(self, **kwargs):
        data = self.initialize(**kwargs)
        if data is not None:
            self.create_nodes(data=data)

    def initialize(self, **kwargs):
        return None

    @property
    def node_type(self):
        return self.get_metadata()["node_type"]

    @node_type.setter
    def node_type(self, value):
        self._node_type = value
        self.set_metadata({"node_type": self._node_type})

    @property
    def name_column(self):
        return self.get_metadata()["name_column"]

    @name_column.setter
    def name_column(self, value):
        self.set_metadata({"name_column": value})

    @property
    def n_nodes(self):
        return self.read_nodes(columns=["id"]).num_rows

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
        tmp_str += f"NODE STORE SUMMARY\n"
        tmp_str += f"{'=' * 60}\n"
        tmp_str += f"Node type: {self.node_type}\n"
        tmp_str += f"• Number of nodes: {self.n_nodes}\n"
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
        tmp_str += f"NODE DETAILS\n"
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

    def create_nodes(
        self,
        data: Union[List[dict], dict, pd.DataFrame, pa.Table],
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
        num_records = len(data) if isinstance(data, (list, pd.DataFrame)) else 1
        logger.info(f"Creating {num_records} node records")
        try:
            self.create(**create_kwargs)
            logger.debug("Node creation successful")
        except Exception as e:
            logger.error(f"Failed to create nodes: {str(e)}")
            raise

    def read_nodes(
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
        >>> data = db.read_nodes(ids=[1, 2, 3], columns=['name', 'age'], filters=[pc.field('age') > 18])
        """
        id_msg = f"for IDs {ids[:5]}..." if ids else "for all nodes"
        col_msg = f" columns: {columns}" if columns else ""
        logger.debug(f"Reading nodes {id_msg}{col_msg}")

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
        try:
            result = self.read(**read_kwargs)
            logger.debug(
                f"Successfully read {len(result) if hasattr(result, '__len__') else 'unknown'} records"
            )
            return result
        except Exception as e:
            logger.error(f"Failed to read nodes: {str(e)}")
            raise

    def update_nodes(
        self,
        data: Union[List[dict], dict, pd.DataFrame],
        schema: pa.Schema = None,
        metadata: dict = None,
        fields_metadata: dict = None,
        update_keys: Union[str, List[str]] = "id",
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
        update_keys : str or list of str, optional
            The keys to use for updating the data. If a list, the data must contain a row for each key.
        treat_fields_as_ragged : list of str, optional
            A list of fields to treat as ragged arrays.
        convert_to_fixed_shape : bool, optional
            If True, the ragged arrays will be converted to fixed shape arrays.
        normalize_config : NormalizeConfig, optional
            Configuration for the normalization process, optimizing performance by managing row distribution and file structure.

        Examples
        --------
        >>> db.update_nodes(data=[{'id': 1, 'name': 'John', 'age': 30}, {'id': 2, 'name': 'Jane', 'age': 25}])
        """

        num_records = len(data) if isinstance(data, (list, pd.DataFrame)) else 1
        logger.info(f"Updating {num_records} node records")

        update_kwargs = dict(
            data=data,
            update_keys=update_keys,
            schema=schema,
            metadata=metadata,
            fields_metadata=fields_metadata,
            normalize_config=normalize_config,
            treat_fields_as_ragged=treat_fields_as_ragged,
            convert_to_fixed_shape=convert_to_fixed_shape,
        )
        try:
            self.update(**update_kwargs)
            logger.debug("Node update successful")
        except Exception as e:
            logger.error(f"Failed to update nodes: {str(e)}")
            raise

    def delete_nodes(
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
        if ids:
            logger.info(f"Deleting {len(ids)} nodes")
        if columns:
            logger.info(f"Deleting columns: {columns}")
        try:
            self.delete(ids=ids, columns=columns)
            logger.debug("Node deletion successful")
        except Exception as e:
            logger.error(f"Failed to delete nodes: {str(e)}")
            raise

    def normalize_nodes(self, normalize_config: NormalizeConfig = NormalizeConfig()):
        """
        Normalize the dataset by restructuring files for consistent row distribution.

        This method optimizes performance by ensuring that files in the dataset directory have a consistent number of rows.
        It first creates temporary files from the current dataset and rewrites them, ensuring that no file has significantly
        fewer rows than others, which can degrade performance. This is particularly useful after a large data ingestion,
        as it enhances the efficiency of create, read, update, and delete operations.

        Parameters
        ----------
        normalize_config : NormalizeConfig, optional
            Configuration for the normalization process, optimizing performance by managing row distribution and file structure.

        Returns
        -------
        None
            This function does not return anything but modifies the dataset directory in place.

        Examples
        --------
        from parquetdb.core.parquetdb import NormalizeConfig
        normalize_config=NormalizeConfig(load_format='batches',
                                         max_rows_per_file=5000,
                                         min_rows_per_group=500,
                                         max_rows_per_group=5000,
                                         existing_data_behavior='overwrite_or_ignore',
                                         max_partitions=512)
        >>> db.normalize_nodes(normalize_config=normalize_config)
        """
        logger.info("Starting node store normalization")
        try:
            self.normalize(normalize_config=normalize_config)
            logger.debug("Node store normalization completed")
        except Exception as e:
            logger.error(f"Failed to normalize node store: {str(e)}")
            raise

    def set_node_type(self, new_node_type: str):
        """
        Change the node type of the node store.
        """
        self.rename_dataset(new_node_type, remove_dest=True)
        self.set_metadata(metadata={"node_type": new_node_type})
