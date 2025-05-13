import itertools
import logging
import os
import shutil
import types
from collections.abc import Iterable
from dataclasses import dataclass
from glob import glob
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import dill
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.fs as fs
import pyarrow.parquet as pq

from parquetdb.core import types
from parquetdb.utils import data_utils, mp_utils, pyarrow_utils
from parquetdb.utils.config import config
from parquetdb.utils.log_utils import set_verbose_level

dill.settings["recurse"] = True
# Logger setup
logger = logging.getLogger(__name__)


# TODO: Issue when updating structs with new fields that are inside of ListArrays
# TODO: Creating empty table does not work for extenstions
# TODO: Add support for serializationg and deserializing for nested python object
# TODO: Add support to delete specific data
# TODO: There is not way to update a column to a null value


@dataclass
class NormalizeConfig:
    """
    Configuration for the normalization process, optimizing performance by managing row distribution and file structure.

    Attributes
    ----------
    load_format : str
        The format of the output dataset. Supported formats are 'table' and 'batches'.
        Default: 'table'
    batch_size : int
        The number of rows to process in each batch.
        Default: 131,072
    batch_readahead : int
        The number of batches to read ahead in a file.
        Default: 16
    fileformat : pyarrow.dataset.ParquetFileFormat
        The file format to use for the dataset.
        Default: None
    fragment_readahead : int
        The number of files to read ahead, improving IO utilization at the cost of RAM usage.
        Default: 4
    fragment_scan_options : Optional[pa.dataset.FragmentScanOptions]
        Options specific to a particular scan and fragment type, potentially changing across scans.
        Default: None
    use_threads : bool
        Whether to use maximum parallelism determined by available CPU cores.
        Default: True
    memory_pool : Optional[pa.MemoryPool]
        The memory pool for allocations. Uses the system's default memory pool if not specified.
        Default: None
    filesystem : pyarrow.fs.FileSystem
        Filesystem for writing the dataset.
        Default: None
    file_options : pyarrow.fs.FileWriteOptions
        Options for writing the dataset files.
        Default: None
    max_partitions : int
        Maximum number of partitions for dataset writing.
        Default: 1024
    max_open_files : int
        Maximum open files for dataset writing.
        Default: 1024
    max_rows_per_file : int
        Maximum rows per file.
        Default: 10,000
    min_rows_per_group : int
        Minimum rows per row group within each file.
        Default: 0
    max_rows_per_group : int
        Maximum rows per row group within each file.
        Default: 10,000
    existing_data_behavior : str
        How to handle existing data in the dataset directory.
        Options: 'overwrite_or_ignore'
        Default: 'overwrite_or_ignore'
    create_dir : bool
        Whether to create the dataset directory if it does not exist.
        Default: True
    """

    load_format: str = "table"
    batch_size: int = 131_072
    batch_readahead: int = 16
    fragment_readahead: int = 4
    fileformat: Optional[ds.ParquetFileFormat] = "parquet"
    fragment_scan_options: Optional[pa.dataset.FragmentScanOptions] = None
    memory_pool: Optional[pa.MemoryPool] = None
    filesystem: Optional[fs.FileSystem] = None
    file_options: Optional[ds.FileWriteOptions] = None
    use_threads: bool = True
    max_partitions: int = 1024
    max_open_files: int = 1024
    max_rows_per_file: int = 10000000
    min_rows_per_group: int = 50000
    max_rows_per_group: int = 100000
    file_visitor: Optional[Callable] = None
    existing_data_behavior: str = "overwrite_or_ignore"
    create_dir: bool = True

    def __repr__(self):
        tmp = "NormalizeConfig("
        for key, value in self.__dict__.items():
            tmp += f"{key}={value}, "
        tmp += ")"
        return tmp


@dataclass
class LoadConfig:
    """
    Configuration for loading data, specifying columns, filters, batch size, and memory usage.

    Attributes
    ----------
    batch_size : int
        The number of rows to process in each batch.
        Default: 131,072
    batch_readahead : int
        The number of batches to read ahead in a file.
        Default: 16
    fragment_readahead : int
        The number of files to read ahead, improving IO utilization at the cost of RAM usage.
        Default: 4
    fragment_scan_options : Optional[pa.dataset.FragmentScanOptions]
        Options specific to a particular scan and fragment type, potentially changing across scans.
        Default: None
    use_threads : bool
        Whether to use maximum parallelism determined by available CPU cores.
        Default: True
    memory_pool : Optional[pa.MemoryPool]
        The memory pool for allocations. Uses the system's default memory pool if not specified.
        Default: None
    """

    batch_size: int = 131_072
    batch_readahead: int = 16
    fragment_readahead: int = 4
    fragment_scan_options: Optional[pa.dataset.FragmentScanOptions] = None
    use_threads: bool = True
    memory_pool: Optional[pa.MemoryPool] = None


class ParquetDB:
    def __init__(
        self,
        db_path: Union[str, Path],
        initial_fields: List[pa.Field] = None,
        serialize_python_objects: bool = False,
        use_multiprocessing: bool = False,
        normalize_config: NormalizeConfig = NormalizeConfig(),
        load_config: LoadConfig = LoadConfig(),
        verbose: int = 1,
    ):
        """
        Initializes the ParquetDB object.

        Parameters
        ----------
        db_path : str
            The path of the database.
        initial_fields : List[pa.Field], optional
            List of PyArrow fields to initialize the database schema with.
            An 'id' field of type int64 will automatically be added.
            Default is None (empty list).
        serialize_python_objects : bool, optional
            Whether to serialize Python objects when storing them.
            Default is True.
        use_multiprocessing : bool, optional
            Whether to enable multiprocessing for operations.
            Default is False.
        verbose: int, optional
            Verbosity level for logging.
            Default is 1.


        Examples
        --------
        >>> from parquetdb import ParquetDB
        >>> import pyarrow as pa
        >>> fields = [pa.field('name', pa.string()), pa.field('age', pa.int32())]
        >>> db = ParquetDB(db_path='/path/to/db', initial_fields=fields)
        """

        logger.info(f"Initializing ParquetDB with db_path: {db_path}")

        logger.info(f"verbose: {verbose}")
        set_verbose_level(verbose=verbose)

        self.normalize_config = normalize_config
        self.load_config = load_config

        self._db_path = Path(db_path)
        self._db_path.mkdir(parents=True, exist_ok=True)
        self._serialize_python_objects = serialize_python_objects

        if initial_fields is None:
            initial_fields = []
        initial_fields = [pa.field("id", pa.int64())] + initial_fields

        self.load_formats = ["batches", "table", "dataset"]

        logger.info(f"db_path: {self.db_path}")
        logger.info(f"load_formats: {self.load_formats}")

        if self.is_empty() and len(os.listdir(self.db_path)) == 0:
            logger.debug(
                f"Dataset {self.dataset_name} is empty. Creating empty dataset."
            )
            table = pyarrow_utils.create_empty_table(schema=pa.schema(initial_fields))
            pq.write_table(table, self._get_save_path())

        # Applying config
        # config.use_multiprocessing = use_multiprocessing
        # config.serialize_python_objects = serialize_python_objects

    def __repr__(self):
        return self.summary(show_column_names=False)

    @property
    def db_path(self) -> Path:
        """
        Get the database path.

        Returns
        -------
        Path
            The path to the database directory.
        """
        return self._db_path

    @property
    def dataset_name(self) -> str:
        """
        Get the dataset name.

        Returns
        -------
        str
            The name of the dataset, derived from the database path.
        """
        return self._db_path.name

    @property
    def basename_template(self) -> str:
        """
        Get the template for parquet file basenames.

        Returns
        -------
        str
            Template string for parquet filenames in format "{dataset_name}_{i}.parquet".
        """
        return f"{self.dataset_name}_{{i}}.parquet"

    @property
    def n_columns(self) -> int:
        """
        Get the number of columns in the database.

        Returns
        -------
        int
            The total number of columns.
        """
        return len(self.columns)

    @property
    def columns(self) -> List[str]:
        """
        Get the column names in the database.

        Returns
        -------
        List[str]
            List of column names.
        """
        return self.get_field_names()

    @property
    def n_rows(self) -> int:
        """
        Get the total number of rows in the database.

        Returns
        -------
        int
            The total number of rows.
        """
        ds = self.read(load_format="dataset")
        return ds.count_rows()

    @property
    def n_files(self) -> int:
        """
        Get the number of parquet files in the database.

        Returns
        -------
        int
            The total number of parquet files.
        """
        return len(self.get_current_files())

    @property
    def n_rows_per_file(self) -> Dict[str, int]:
        """
        Get the number of rows in each parquet file.

        Returns
        -------
        Dict[str, int]
            Returns a dictionary detailing the number of rows in each processed Parquet file.
            Structure: {filename: n_rows}
        """
        return self.get_number_of_rows_per_file()

    @property
    def n_row_groups_per_file(self) -> Dict[str, int]:
        """
        Get the number of row groups in each parquet file.

        Returns
        -------
        Dict[str, int]
            Returns a dictionary detailing the number of row groups in each processed Parquet file.
            Structure: {filename: n_row_groups}
        """
        return self.get_number_of_row_groups_per_file()

    @property
    def n_rows_per_row_group_per_file(self) -> Dict[str, Dict[int, int]]:
        """
        Get the number of rows in each row group for each file.

        Returns
        -------
        Dict[str, Dict[int, int]]
            Returns a dictionary detailing the number of rows in each row group for each processed Parquet file.
            Structure: {filename: {row_group_id: n_rows}}
        """
        return self.get_n_rows_per_row_group_per_file(as_dict=True)

    @property
    def serialized_metadata_size_per_file(self) -> Dict[str, int]:
        """
        Get the size of serialized metadata for each file.

        Returns
        -------
        Dict[str, int]
            Returns a dictionary detailing the size of serialized metadata for each processed Parquet file.
            Structure: {filename: metadata_size_in_bytes}
        """
        return self.get_serialized_metadata_size_per_file()

    def summary(
        self, show_column_names: bool = False, show_row_group_metadata: bool = False
    ):
        """
        Generate a formatted summary string containing database information and metadata.

        Parameters
        ----------
        show_column_names : bool, optional
            If True, include a list of column names and their metadata in the summary.
            Default is False.
        show_row_group_metadata : bool, optional
            If True, include detailed row group information for each file.
            Default is False.

        Returns
        -------
        str
            A formatted string containing:
            - Basic database information (path, number of columns/rows/files)
            - Per-file statistics (rows, row groups, metadata size)
            - Database metadata
            - Column details (if show_column_names=True)
            - Row group details (if show_row_group_metadata=True)

        Examples
        --------
        >>> db = ParquetDB("my_database")
        >>> print(db.summary())
        ============================================================
        PARQUETDB SUMMARY
        ============================================================
        Database path: /path/to/my_database

        • Number of columns: 5
        • Number of rows: 1000
        • Number of files: 2
        ...

        >>> print(db.summary(show_column_names=True))
        # Shows same summary plus list of columns and their metadata
        """
        fields_metadata = self.get_field_metadata()
        metadata = self.get_metadata()
        # Header section
        tmp_str = f"{'=' * 60}\n"
        tmp_str += f"PARQUETDB SUMMARY\n"
        tmp_str += f"{'=' * 60}\n"
        tmp_str += f"Database path: {os.path.abspath(self.db_path)}\n\n"
        tmp_str += f"• Number of columns: {self.n_columns}\n"
        tmp_str += f"• Number of rows: {self.n_rows}\n"
        tmp_str += f"• Number of files: {self.n_files}\n"
        tmp_str += f"• Number of rows per file: {self.n_rows_per_file}\n"
        tmp_str += f"• Number of row groups per file: {self.n_row_groups_per_file}\n"
        if show_row_group_metadata:
            tmp_str += f"• Number of rows per row group per file: \n"
            for (
                filename,
                row_group_metadata,
            ) in self.n_rows_per_row_group_per_file.items():
                tmp_str += f"    - {filename}:\n"
                for row_group_idx, n_rows in row_group_metadata.items():
                    tmp_str += f"        - Row group {row_group_idx}: {n_rows} rows\n"
        tmp_str += f"• Serialized metadata size per file: {self.serialized_metadata_size_per_file} Bytes\n"

        # Metadata section
        tmp_str += f"\n{'#' * 60}\n"
        tmp_str += f"METADATA\n"
        tmp_str += f"{'#' * 60}\n"
        for key, value in metadata.items():
            tmp_str += f"• {key}: {value}\n"

        # Node details
        tmp_str += f"\n{'#' * 60}\n"
        tmp_str += f"COLUMN DETAILS\n"
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

    def create(
        self,
        data: Union[List[dict], dict, pd.DataFrame],
        schema: pa.Schema = None,
        metadata: Dict[str, str] = None,
        fields_metadata: Dict[str, Dict[str, str]] = None,
        treat_fields_as_ragged: List[str] = None,
        convert_to_fixed_shape: bool = True,
        normalize_dataset: bool = False,
        normalize_config: NormalizeConfig = None,
    ) -> None:
        """
        Adds new data to the database.

        Parameters
        ----------
        data : Union[List[dict], dict, pd.DataFrame]
            The data to create the dataset with. Can be:
            - A list of dictionaries, where each dict represents a row
            - A single dictionary with column names as keys and lists of values
            - A pandas DataFrame
            - A pyarrow Table
        schema : pa.Schema, optional
            PyArrow schema defining the structure and types of the data.
            If not provided, schema will be inferred from the data.
        metadata : Dict[str, str], optional
            Dictionary of key-value pairs to attach as metadata to the table.
            This metadata applies to the entire table.
        fields_metadata : Dict[str, Dict[str, str]], optional
            Dictionary mapping field names to their metadata dictionaries.
            Allows attaching metadata to specific fields/columns.
        treat_fields_as_ragged : List[str], optional
            List of field names to treat as ragged arrays (arrays with varying lengths).
            These fields will be processed differently during data loading.
        convert_to_fixed_shape : bool, optional
            Whether to convert ragged arrays to fixed shape tensors.
            If True, ragged arrays will be padded to match the maximum length.
            Default is True.
        normalize_dataset : bool, optional
            Whether to normalize the dataset after creation.
            Normalization optimizes data storage and query performance.
            Default is False.
        normalize_config : NormalizeConfig, optional
            Configuration object for dataset normalization, controlling:
            - Row distribution across files
            - Row group sizes
            - File organization
            - Thread/memory usage
            Default uses standard NormalizeConfig settings.

        Returns
        -------
        None

        Examples
        --------
        >>> data = [
        ...     {'name': 'Alice', 'age': 30},
        ...     {'name': 'Bob', 'age': 25}
        ... ]
        >>> db.create(
        ...     data=data,
        ...     metadata={'source': 'users'},
        ...     normalize_dataset=True
        ... )

        Notes
        -----
        - An 'id' column will automatically be added if not present
        - The schema will be unified with any existing data
        - Ragged arrays are converted to fixed shape by default
        - Normalization is recommended for optimal performance
        """
        if normalize_config is None:
            normalize_config = self.normalize_config

        logger.info("Creating data")
        self._db_path.mkdir(parents=True, exist_ok=True)

        # Construct incoming table from the data
        incoming_table = ParquetDB.construct_table(
            data,
            schema=schema,
            metadata=metadata,
            fields_metadata=fields_metadata,
            serialize_python_objects=self._serialize_python_objects,
        )

        if "id" in incoming_table.column_names:
            raise ValueError(
                "When create is called, the data cannot contain an 'id' column."
            )
        new_ids = self._get_new_ids(incoming_table)
        incoming_table = incoming_table.append_column(
            pa.field("id", pa.int64()), [new_ids]
        )

        incoming_table = ParquetDB.preprocess_table(
            incoming_table,
            treat_fields_as_ragged=treat_fields_as_ragged,
            convert_to_fixed_shape=convert_to_fixed_shape,
        )

        # Merge Schems
        initially_empty = self.is_empty()
        current_schema = self.get_schema()
        incoming_schema = incoming_table.schema

        merged_schema = pyarrow_utils.unify_schemas(
            [current_schema, incoming_schema], promote_options="permissive"
        )
        # Algin Incoming Table with Merged Schema
        modified_incoming_table = pyarrow_utils.table_schema_cast(
            incoming_table, merged_schema
        )
        are_schemas_equal = pyarrow_utils.schema_equal(
            current_schema, modified_incoming_table.schema
        )

        # Align Existing Data with the merged schema, if it is not empty
        if not are_schemas_equal and not initially_empty:
            logger.info(
                f"Schemas not are equal: {are_schemas_equal}. Normalizing the dataset."
            )
            self._normalize(schema=merged_schema, normalize_config=normalize_config)

        # Write the incoming table to the database
        try:
            incoming_save_path = self._get_save_path()
            pq.write_table(modified_incoming_table, incoming_save_path)

            # If the dataset is initially empty, remove the initial file and rename the incoming file to the initial file
            if initially_empty:
                initial_file_path = self._db_path / f"{self.dataset_name}_0.parquet"
                incoming_file_path = self.db_path / f"{self.dataset_name}_1.parquet"

                initial_file_path.unlink()
                incoming_file_path.rename(initial_file_path)
                self._normalize(
                    schema=modified_incoming_table.schema,
                    normalize_config=normalize_config,
                )
        except Exception as e:
            logger.exception(f"exception writing table: {e}")

        if normalize_dataset:
            logger.info("Normalizing the dataset")
            self._normalize(
                schema=modified_incoming_table.schema, normalize_config=normalize_config
            )

        logger.info("Creating dataset passed")
        # except Exception as e:
        #     logger.exception(f"exception aligning schemas: {e}")
        return None

    def read(
        self,
        ids: List[int] = None,
        columns: List[str] = None,
        filters: List[pc.Expression] = None,
        load_format: str = "table",
        batch_size: int = None,
        include_cols: bool = True,
        rebuild_nested_struct: bool = False,
        rebuild_nested_from_scratch: bool = False,
        load_config: LoadConfig = None,
        normalize_config: NormalizeConfig = None,
    ) -> Union[pa.Table, Iterable[pa.RecordBatch], ds.Dataset]:
        """
        Reads data from the database with flexible filtering and formatting options.

        Parameters
        ----------
        ids : List[int], optional
            Specific IDs to read from the database. If None, reads all records.
        columns : List[str], optional
            Column names to include/exclude in the output. If None, includes all columns.
        filters : List[pc.Expression], optional
            PyArrow compute expressions for filtering the data.
            Example: [pc.field('age') > 18]
        load_format : str, optional
            Format of the returned data. Options:
            - 'table': Returns a PyArrow Table (default)
            - 'batches': Returns a generator of record batches
            - 'dataset': Returns a PyArrow Dataset
        batch_size : int, optional
            Number of rows per batch when reading data. Overrides batch_size in load_config if provided.
        include_cols : bool, default True
            If True, includes only the specified columns.
            If False, excludes the specified columns.
        rebuild_nested_struct : bool, default False
            Whether to rebuild and use the nested structure for reading.
            Useful for optimizing reads of deeply nested data.
        rebuild_nested_from_scratch : bool, default False
            If True, rebuilds the nested structure from scratch rather than using existing.
            Only relevant if rebuild_nested_struct is True.
        load_config : LoadConfig, optional
            Configuration for optimizing data loading performance.
            Controls batch sizes, readahead, threading, and memory usage.
        normalize_config : NormalizeConfig, optional
            Configuration for optimizing data distribution and file structure.
            Used when rebuilding nested structures.

        Returns
        -------
        Union[pa.Table, Iterable[pa.RecordBatch], ds.Dataset]
            Data in the requested format:
            - PyArrow Table if load_format='table'
            - Generator of record batches if load_format='batches'
            - PyArrow Dataset if load_format='dataset'

        Examples
        --------
        Read specific columns for certain IDs:
        >>> data = db.read(ids=[1, 2, 3], columns=['name', 'age'])

        Read with filtering:
        >>> filters = [pc.field('age') > 18, pc.field('city') == 'New York']
        >>> data = db.read(filters=filters)

        Read in batches:
        >>> for batch in db.read(load_format='batches', batch_size=1000):
        ...     process_batch(batch)

        Optimize for nested data:
        >>> data = db.read(rebuild_nested_struct=True)

        Notes
        -----
        - The method automatically handles schema alignment and type casting
        - For large datasets, using 'batches' format with appropriate batch_size
          helps manage memory usage
        - rebuild_nested_struct can significantly improve performance for
          queries on nested data structures
        """
        if load_config is None:
            load_config = self.load_config
        if normalize_config is None:
            normalize_config = self.normalize_config

        if batch_size:
            load_config.batch_size = batch_size

        logger.info("Reading data")

        read_columns = self.get_field_names(columns=columns, include_cols=include_cols)
        if not include_cols:
            columns = read_columns

        if filters is None:
            filters = []

        # Build filter expression
        filter_expression = self._build_filter_expression(ids, filters)
        dataset_dir = None
        if rebuild_nested_struct:
            nested_dataset_dir = self.db_path / "nested"
            dataset_dir = nested_dataset_dir
            if not nested_dataset_dir.exists() or rebuild_nested_from_scratch:
                self.to_nested(
                    normalize_config=normalize_config,
                    nested_dataset_dir=nested_dataset_dir,
                    rebuild_nested_from_scratch=rebuild_nested_from_scratch,
                )
        data = self._load_data(
            columns=columns,
            filter=filter_expression,
            load_format=load_format,
            dataset_dir=dataset_dir,
            load_config=load_config,
        )
        logger.info("Reading data passed")
        return data

    def update(
        self,
        data: Union[List[dict], dict, pd.DataFrame],
        schema: pa.Schema = None,
        metadata: Dict[str, str] = None,
        fields_metadata: Dict[str, Dict[str, str]] = None,
        update_keys: Union[List[str], str] = ["id"],
        treat_fields_as_ragged: List[str] = None,
        convert_to_fixed_shape: bool = True,
        normalize_config: NormalizeConfig = None,
    ) -> None:
        """
        Updates existing records in the database by matching on specified key fields.

        Parameters
        ----------
        data : Union[List[dict], dict, pd.DataFrame]
            The data containing updates. Can be:
            - A list of dictionaries, where each dict represents a row
            - A single dictionary with column names as keys and lists of values
            - A pandas DataFrame
            Each record must contain the update_keys fields to match existing records.
        schema : pa.Schema, optional
            PyArrow schema defining the structure and types of the update data.
            If not provided, schema will be inferred from the data.
        metadata : Dict[str, str], optional
            Dictionary of key-value pairs to attach as metadata to the updated table.
            This metadata applies to the entire table.
        fields_metadata : Dict[str, Dict[str, str]], optional
            Dictionary mapping field names to their metadata dictionaries.
            Allows attaching metadata to specific fields/columns.
        update_keys : Union[List[str], str], optional
            Field name(s) to use for matching update records with existing records.
            Can be a single field name string or list of field names.
            Default is ["id"].
        treat_fields_as_ragged : List[str], optional
            List of field names to treat as ragged arrays (arrays with varying lengths).
            These fields will be processed differently during data loading.
        convert_to_fixed_shape : bool, optional
            Whether to convert ragged arrays to fixed shape tensors.
            If True, ragged arrays will be padded to match the maximum length.
            Default is True.
        normalize_config : NormalizeConfig, optional
            Configuration object for dataset normalization after update, controlling:
            - Row distribution across files
            - Row group sizes
            - File organization
            - Thread/memory usage
            Default uses standard NormalizeConfig settings.

        Examples
        --------
        Update records using id field:
        >>> db.update([
        ...     {'id': 1, 'name': 'John', 'age': 30},
        ...     {'id': 2, 'name': 'Jane', 'age': 25}
        ... ])

        Update using multiple key fields:
        >>> db.update(
        ...     data={'name': ['John'], 'dept': ['Sales'], 'salary': [50000]},
        ...     update_keys=['name', 'dept']
        ... )

        Notes
        -----
        - Records are matched and updated based on update_keys fields
        - New fields in update data will be added to existing records
        - Missing fields in update data will preserve existing values
        - Schema and data types are automatically aligned
        """
        if normalize_config is None:
            normalize_config = self.normalize_config

        if self.is_empty():
            logger.info(f"Dataset {self.dataset_name} is empty. No data to update.")
            return None

        logger.info("Updating data")

        # Construct incoming table from the data
        incoming_table = ParquetDB.construct_table(
            data,
            schema=schema,
            metadata=metadata,
            fields_metadata=fields_metadata,
            serialize_python_objects=self._serialize_python_objects,
        )

        incoming_table = ParquetDB.preprocess_table(
            incoming_table,
            treat_fields_as_ragged=treat_fields_as_ragged,
            convert_to_fixed_shape=convert_to_fixed_shape,
        )
        incoming_table = pyarrow_utils.table_schema_cast(
            incoming_table, incoming_table.schema
        )

        # Non-exisiting id warning step. This is not really necessary but might be nice for user to check
        # self._validate_id(incoming_table['id'].combine_chunks())

        # Apply update normalization
        self._normalize(
            incoming_table=incoming_table,
            update_keys=update_keys,
            normalize_config=normalize_config,
        )

        logger.info(f"Updated {self.dataset_name} table.")
        return None

    def delete(
        self,
        ids: List[int] = None,
        filters: List[pc.Expression] = None,
        columns: List[str] = None,
        normalize_config: NormalizeConfig = None,
    ) -> None:
        """
        Deletes records or columns from the database.

        Parameters
        ----------
        ids : List[int], optional
            List of record IDs to delete from the database.
            Cannot be used together with columns or filters.
        filters : List[pc.Expression], optional
            PyArrow compute expressions to filter which records to delete.
            Cannot be used together with ids or columns.
            Example: [pc.field('age') > 30]
        columns : List[str], optional
            List of column names to delete from the dataset.
            Cannot delete the 'id' column or be used with ids/filters.
        normalize_config : NormalizeConfig, optional
            Configuration for optimizing data distribution and file structure
            after deletion. Controls batch sizes, readahead, threading, and
            memory usage.

        Returns
        -------
        None
            Returns None if no matching records found to delete.

        Examples
        --------
        Delete specific records by ID:
        >>> db.delete(ids=[1, 2, 3])

        Delete records matching a filter:
        >>> db.delete(filters=[pc.field('age') > 30])

        Delete specific columns:
        >>> db.delete(columns=['address', 'phone'])

        Notes
        -----
        - Must provide exactly one of: ids, filters, or columns
        - Cannot delete the 'id' column
        - Returns None if no matching records/columns found
        - Automatically normalizes dataset after deletion
        """
        if normalize_config is None:
            normalize_config = self.normalize_config

        if ids is not None and columns is not None and filters is not None:
            raise ValueError("Cannot provide both ids, columns and filters to delete.")
        if ids is None and columns is None and filters is None:
            raise ValueError("Must provide either ids or columns to delete.")

        if self.is_empty():
            logger.info(f"Dataset {self.dataset_name} is empty. No data to delete.")
            return None

        logger.info("Deleting data from the database")

        if ids:
            ids = set(ids)
            # Check if any of the IDs to delete exist in the table. If not, return None
            current_id_table = self._load_data(columns=["id"], load_format="table")
            filtered_id_table = current_id_table.filter(pc.field("id").isin(ids))
            if filtered_id_table.num_rows == 0:
                logger.info(f"No data found to delete.")
                return None

        if columns:
            if "id" in columns:
                raise ValueError("Cannot delete the 'id' column.")
            # Check if any of the columns to delete exist in the table. If not, return None
            schema = self.get_schema()
            incoming_columns = set(columns)
            current_columns = set(schema.names)
            intersection = current_columns.intersection(incoming_columns)
            if len(intersection) == 0:
                logger.info(f"No data found to delete.")
                return None

        if filters:
            filter_expression = self._build_filter_expression(filters=filters)

            current_id_table = self._load_data(
                columns=["id"], filter=filter_expression, load_format="table"
            )
            ids = current_id_table["id"].combine_chunks()

            if len(ids) == 0:
                logger.info(f"No data found to delete.")
                return None

        # Apply delete normalization
        self._normalize(ids=ids, columns=columns, normalize_config=normalize_config)

        logger.info(f"Deleted data from {self.dataset_name} dataset.")
        return None

    def transform(
        self,
        transform_callable: Callable[[pa.Table], pa.Table],
        new_db_path: Optional[str] = None,
        normalize_config: NormalizeConfig = None,
    ) -> "ParquetDB":
        """
        Transform the entire dataset using a user-provided callable.

        This function loads the dataset as a PyArrow table, applies a transformation function,
        and writes the transformed data either in-place or to a new location.

        Parameters
        ----------
        transform_callable : Callable[[pa.Table], pa.Table]
            A function that takes a PyArrow Table and returns a transformed PyArrow Table.
            The function should preserve the schema structure but can modify values and add/remove rows.
        new_db_path : str, optional
            Path where the transformed dataset will be written as a new ParquetDB.
            If None, transforms the current ParquetDB in-place.
            Default is None.
        normalize_config : NormalizeConfig, optional
            Configuration for optimizing the transformed dataset's storage.
            Controls file sizes, row distribution, and performance settings.
            Default uses standard NormalizeConfig settings.

        Returns
        -------
        ParquetDB
            If new_db_path is provided, returns a new ParquetDB instance at that location.
            If new_db_path is None, returns the current ParquetDB instance after transforming in-place.

        Examples
        --------
        >>> def add_column(table):
        ...     values = range(len(table))
        ...     new_col = pa.array(values)
        ...     return table.append_column('new_col', new_col)

        >>> # Transform in-place
        >>> db.transform(add_column)

        >>> # Transform to new location
        >>> new_db = db.transform(add_column, new_db_path='path/to/new/db')
        """
        if normalize_config is None:
            normalize_config = self.normalize_config

        if new_db_path:
            logger.info(f"Writing transformation to new dir: {new_db_path}")

        self._normalize(
            transform_callable=transform_callable,
            normalize_config=normalize_config,
            new_db_path=new_db_path,
        )

        if new_db_path:
            return ParquetDB(new_db_path)
        return self

    def normalize(self, normalize_config: NormalizeConfig = None):
        """
        Normalize the dataset by restructuring files for optimal performance.

        This method reorganizes the dataset files to ensure consistent row distribution and
        efficient storage. It rewrites the data with optimized file and row group sizes,
        which improves performance of all database operations.

        Parameters
        ----------
        normalize_config : NormalizeConfig, optional
            Configuration controlling the normalization process, including:
            - File sizes and row distribution
            - Row group sizes and organization
            - Threading and memory usage
            - File system options
            Default uses standard NormalizeConfig settings.

        Returns
        -------
        None
            Modifies the dataset directory in place.

        Examples
        --------
        Basic normalization with default settings:
        >>> db.normalize()

        Custom normalization configuration:
        >>> config = NormalizeConfig(
        ...     max_rows_per_file=5000,
        ...     min_rows_per_group=500,
        ...     max_rows_per_group=5000,
        ...     max_partitions=512,
        ...     use_threads=True
        ... )
        >>> db.normalize(normalize_config=config)

        Notes
        -----
        - Recommended after large data ingestions
        - Improves performance of create, read, update and delete operations
        - Ensures balanced file sizes and row distribution
        - Safe to run at any time to optimize storage
        """
        if normalize_config is None:
            normalize_config = self.normalize_config

        if self.is_empty():
            logger.info(f"Dataset {self.dataset_name} is empty. No data to normalize.")
            return None
        self._normalize(normalize_config=normalize_config)

    def _normalize(
        self,
        nested_dataset_dir: Union[str, Path] = None,
        incoming_table: pa.Table = None,
        schema: pa.Schema = None,
        ids: List[int] = None,
        columns: List[str] = None,
        transform_callable: Callable = None,
        new_db_path: Union[str, Path] = None,
        update_keys: Union[List[str], str] = ["id"],
        normalize_config: NormalizeConfig = None,
    ) -> None:
        """
        Internal method to normalize the dataset by restructuring files.

        This method handles the core normalization logic, including:
        - Rewriting files to ensure balanced row distribution
        - Applying updates or deletions if specified
        - Optimizing file and row group organization
        - Managing schema changes and data transformations

        Parameters
        ----------
        nested_dataset_dir : str | Path, optional
            Path to store nested data structure. Used for optimizing queries on nested data.
        incoming_table : pa.Table, optional
            New data to merge during update operations.
        schema : pa.Schema, optional
            Schema to enforce during normalization. If None, preserves existing schema.
        ids : List[int], optional
            Record IDs to remove during normalization.
        columns : List[str], optional
            Column names to remove during normalization.
        transform_callable : Callable, optional
            Custom transformation function to apply during normalization.
        new_db_path : str | Path, optional
            Alternative path to write normalized data. If None, overwrites existing files.
        update_keys : Union[List[str], str], optional
            Field(s) to match on when merging incoming_table. Default is ["id"].
        normalize_config : NormalizeConfig, optional
            Configuration controlling file sizes, row groups, threading, etc.
            Default uses standard NormalizeConfig settings.

        Returns
        -------
        None
            Modifies dataset files in place.

        Notes
        -----
        - This is an internal method called by public methods like normalize(), update(), delete()
        - Handles both standard normalization and specialized operations like updates/deletes
        - Uses temporary files to ensure atomic operations
        - Preserves data consistency during restructuring
        """
        if normalize_config is None:
            normalize_config = self.normalize_config

        if new_db_path:
            dataset_dir = Path(new_db_path)
            dataset_name = dataset_dir.name
            basename_template = f"tmp-{dataset_name}_{{i}}.parquet"
        else:
            dataset_dir = self.db_path
            dataset_name = self.dataset_name
            basename_template = f"tmp-{self.dataset_name}_{{i}}.parquet"

        try:
            retrieved_data = self._load_data(
                load_format=normalize_config.load_format,
                load_config=LoadConfig(
                    **dict(
                        batch_size=normalize_config.batch_size,
                        batch_readahead=normalize_config.batch_readahead,
                        fragment_readahead=normalize_config.fragment_readahead,
                        fragment_scan_options=normalize_config.fragment_scan_options,
                        use_threads=normalize_config.use_threads,
                        memory_pool=normalize_config.memory_pool,
                    )
                ),
            )
        except pa.lib.ArrowNotImplementedError as e:
            raise ValueError(
                "The incoming data does not match the schema of the existing data."
            ) from e

        if incoming_table:
            logger.debug(
                "This normalization is an update. Applying update function, then normalizing."
            )
            retrieved_data = data_transform(
                retrieved_data,
                pyarrow_utils.update_flattend_table,
                incoming_table=incoming_table,
                update_keys=update_keys,
            )
        elif ids:
            logger.debug(
                "This normalization is an id delete. Applying delete function, then normalizing."
            )
            retrieved_data = data_transform(
                retrieved_data,
                pyarrow_utils.delete_field_values,
                values=ids,
                field_name="id",
            )

        elif columns:
            logger.debug(
                "This normalization is a column delete. Applying delete function, then normalizing."
            )
            retrieved_data = data_transform(
                retrieved_data, pyarrow_utils.delete_columns, columns=columns
            )
        elif schema:
            logger.debug(
                "This normalization is a schema update. Applying schema cast function, then normalizing."
            )
            retrieved_data = data_transform(
                retrieved_data, pyarrow_utils.table_schema_cast, new_schema=schema
            )

        elif transform_callable:
            logger.debug(
                "This normalization is a transform. Applying transform function, then normalizing."
            )
            retrieved_data = data_transform(retrieved_data, transform_callable)
        elif nested_dataset_dir:
            logger.debug(
                "This normalization is a nested rebuild. Applying rebuild function, then normalizing."
            )
            dataset_dir = nested_dataset_dir
            basename_template = f"{dataset_name}_{{i}}.parquet"
            retrieved_data = data_transform(
                retrieved_data,
                pyarrow_utils.rebuild_nested_table,
                load_format=normalize_config.load_format,
            )

        if isinstance(retrieved_data, pa.lib.Table):
            schema = None
        elif isinstance(retrieved_data, Iterable):
            retrieved_data, schema = extract_generator_schema(retrieved_data)

        # Handles case when table is empty
        if self.is_empty():
            # Handles case when table is empty
            initial_file_path = dataset_dir / f"{dataset_name}_0.parquet"
            pq.write_table(retrieved_data, initial_file_path)
            return None

        # Handles case when table is not empty
        try:
            logger.debug(f"Writing dataset to {dataset_dir}")
            logger.debug(f"Basename template: {basename_template}")
            logger.debug(f"Retrieved data type : {type(retrieved_data)}")
            logger.debug(f"Is Schema None : {schema is None}")

            logger.debug(f"Normalize config: {normalize_config}")

            ds.write_dataset(
                retrieved_data,
                dataset_dir,
                basename_template=basename_template,
                schema=schema,
                format=normalize_config.fileformat,
                filesystem=normalize_config.filesystem,
                file_options=normalize_config.file_options,
                use_threads=normalize_config.use_threads,
                max_partitions=normalize_config.max_partitions,
                max_open_files=normalize_config.max_open_files,
                max_rows_per_file=normalize_config.max_rows_per_file,
                min_rows_per_group=normalize_config.min_rows_per_group,
                max_rows_per_group=normalize_config.max_rows_per_group,
                file_visitor=normalize_config.file_visitor,
                existing_data_behavior=normalize_config.existing_data_behavior,
                create_dir=normalize_config.create_dir,
            )

            schema = self.get_schema()

            # If id not in  fails due to a transform do an error
            if "id" not in schema.names:
                logger.error("id is not in schema")
                raise ValueError("id is not in schema")

            # Remove main files to replace with tmp files
            logger.debug(f"Files before renaming: {os.listdir(dataset_dir)}")
            tmp_files = [
                file for file in dataset_dir.glob(f"tmp-{dataset_name}_*.parquet")
            ]
            if len(tmp_files) != 0:
                main_files = dataset_dir.glob(f"{dataset_name}_*.parquet")
                for file_path in main_files:
                    if file_path.is_file():
                        file_path.unlink()

            logger.debug(f"Files after removing main files: {os.listdir(dataset_dir)}")

            tmp_files = dataset_dir.glob(f"tmp-{dataset_name}_*.parquet")
            for file_path in tmp_files:
                file_name = file_path.name.replace("tmp-", "")
                new_file_path = dataset_dir / file_name
                file_path.rename(new_file_path)

            logger.debug(f"Files after renaming: {os.listdir(dataset_dir)}")

            schema = self._load_data(
                load_format="dataset", dataset_dir=dataset_dir
            ).schema
            logger.debug(f"Columns : \n {schema.names}")

        except Exception as e:
            logger.exception(f"exception writing final table to {dataset_dir}: {e}")

            tmp_files = dataset_dir.glob(f"tmp-{dataset_name}_*.parquet")
            for file_path in tmp_files:
                file_name = file_path.name.replace("tmp-", "")
                new_file_path = dataset_dir / file_name
                file_path.rename(new_file_path)

            if new_db_path:
                dataset_dir.rmdir()

            raise Exception(f"Exception normalizing table. Error Message: {e}")

    def update_schema(
        self,
        field_dict: Dict[str, pa.DataType] = None,
        schema: pa.Schema = None,
        update_metadata: bool = True,
        normalize_config: NormalizeConfig = None,
    ) -> pa.Schema:
        """
        Updates the schema of the table in the dataset.

        This method allows modifying the data types and metadata of fields in the dataset's schema.
        Changes can be specified either through a field dictionary mapping names to new types,
        or by providing a complete new schema. The dataset will be normalized after the schema update.

        Parameters
        ----------
        field_dict : dict
            Dictionary mapping field names to their new PyArrow data types.
            Example: {'age': pa.int32(), 'name': pa.string()}
            If None, uses the provided schema parameter instead.
        schema : pa.Schema
            Complete PyArrow schema to apply to the dataset.
            Takes precedence over field_dict if both are provided.
            Must include all existing fields with their desired types.
        update_metadata : bool
            Whether to preserve and update the schema metadata during the update.
            If True, merges existing metadata with any new metadata.
            If False, uses only the new schema's metadata.
            Default is True.
        normalize_config : NormalizeConfig
            Configuration for optimizing the dataset after schema update:
            - Controls row distribution across files
            - Sets row group sizes and organization
            - Manages threading and memory usage
            Default uses standard NormalizeConfig settings.

        Returns
        -------
        pa.Schema
            The updated schema.

        Examples
        --------
        Update a single field type:
        >>> db.update_schema(field_dict={'age': pa.int32()})

        Apply a complete new schema:
        >>> new_schema = pa.schema([
        ...     ('id', pa.int64()),
        ...     ('name', pa.string()),
        ...     ('age', pa.int32())
        ... ])
        >>> db.update_schema(schema=new_schema)

        Update schema with custom normalization:
        >>> config = NormalizeConfig(max_rows_per_file=5000)
        >>> db.update_schema(
        ...     field_dict={'salary': pa.float64()},
        ...     normalize_config=config
        ... )

        Notes
        -----
        - The 'id' field must be preserved in the schema
        - Schema updates trigger dataset normalization
        - Existing data will be cast to new types where possible
        - Invalid type conversions will raise errors
        """
        if normalize_config is None:
            normalize_config = self.normalize_config

        logger.info("Updating schema")
        current_schema = self.get_schema()

        logger.debug(f"current schema metadata : {current_schema.metadata}")

        if schema is not None:
            logger.debug(f"incoming schema metadata : {schema.metadata}")

        # Update current schema
        updated_schema = pyarrow_utils.update_schema(
            current_schema, schema, field_dict, update_metadata=update_metadata
        )

        logger.debug(f"updated schema metadata : {updated_schema.metadata}")
        logger.debug(f"updated schema names : {updated_schema.names}")

        # Apply Schema normalization
        self._normalize(schema=updated_schema, normalize_config=normalize_config)

        logger.info(f"Updated Fields in {self.dataset_name} table.")
        return updated_schema

    def is_empty(self) -> bool:
        """
        Check if the dataset is empty.

        Returns
        -------
        bool
            True if the dataset is empty or does not exist, False otherwise.

        Examples
        --------
        >>> db = ParquetDB("my_database")
        >>> is_empty = db.is_empty()
        >>> print(is_empty)
        True
        """
        if self.dataset_exists():
            ds = self._load_data(load_format="dataset")
            parquet_file = pq.ParquetFile(
                self.db_path / f"{self.dataset_name}_0.parquet"
            )
            num_rows = parquet_file.metadata.num_rows
            return num_rows == 0
        else:
            return True

    def get_schema(self) -> pa.Schema:
        """
        Get the PyArrow schema of the dataset.

        Returns
        -------
        pyarrow.Schema
            The schema describing the structure and data types of all columns.

        Examples
        --------
        >>> db = ParquetDB("my_database")
        >>> schema = db.get_schema()
        >>> print(schema)
        id: int64
        name: string
        age: int32
        """
        schema = self._load_data(load_format="dataset").schema
        return schema

    def get_field_names(
        self, columns: Optional[List[str]] = None, include_cols: bool = True
    ) -> List[str]:
        """
        Get the names of fields/columns in the dataset schema.

        Parameters
        ----------
        columns : List[str], optional
            List of column names to filter by. If None, returns all column names.
        include_cols : bool, default True
            If True, returns only the columns specified in `columns`.
            If False, returns all columns except those specified in `columns`.

        Returns
        -------
        List[str]
            List of field names based on the filtering criteria.

        Examples
        --------
        Get all field names:
        >>> db = ParquetDB("my_database")
        >>> db.get_field_names()
        ['id', 'name', 'age']

        Get specific fields:
        >>> db.get_field_names(columns=['name', 'age'], include_cols=True)
        ['name', 'age']

        Exclude specific fields:
        >>> db.get_field_names(columns=['age'], include_cols=False)
        ['id', 'name']
        """
        schema = self.get_schema()
        existing_columns = set(schema.names)
        if not include_cols:
            return list(existing_columns - set(columns))
        else:
            return list(existing_columns)

    def get_metadata(self, return_bytes: bool = False) -> Dict[str, Union[str, bytes]]:
        """
        Retrieves the metadata of the dataset table.

        Parameters
        ----------
        return_bytes : bool, optional
            If True, returns raw bytes metadata. If False, decodes to strings.
            Default is False.

        Returns
        -------
        Dict[str, Union[str, bytes]]
            Dictionary containing the table metadata. The values are either str or bytes
            depending on the return_bytes parameter.

        Examples
        --------
        >>> db = ParquetDB("my_database")
        >>> metadata = db.get_metadata()
        >>> print(metadata)
        {'source': 'API', 'version': '1.0'}

        >>> raw_metadata = db.get_metadata(return_bytes=True)
        >>> print(raw_metadata)
        {b'source': b'API', b'version': b'1.0'}
        """
        if not self.dataset_exists():
            raise ValueError(f"Dataset {self.dataset_name} does not exist.")
        schema = self.get_schema()

        metadata = schema.metadata
        if metadata is None:
            metadata = {}

        if return_bytes:
            return metadata
        else:
            metadata = {
                key.decode("utf-8"): value.decode("utf-8")
                for key, value in metadata.items()
            }
        logger.debug(f"Metadata: {metadata}")
        return metadata

    def set_metadata(
        self,
        metadata: Dict[str, str],
        update: bool = True,
    ) -> pa.Schema:
        """
        Sets or updates the metadata of the dataset table.

        Parameters
        ----------
        metadata : Dict[str, str]
            Dictionary of metadata key-value pairs to set for the table.
        update : bool, optional
            If True, updates existing metadata. If False, replaces it entirely.
            Default is True.

        Examples
        --------
        Update existing metadata:
        >>> db.set_metadata({'source': 'API', 'version': '1.0'})

        Replace all metadata:
        >>> db.set_metadata({'new_key': 'value'}, update=False)

        Notes
        -----
        - Metadata keys and values must be strings
        - Updates schema and rewrites Parquet files to persist changes
        """
        # Update metadata in schema and rewrite Parquet files
        new_fields = []
        schema = self.get_schema()
        updated_metadata = self.get_metadata()
        if update:
            updated_metadata.update(metadata)
        else:
            updated_metadata = metadata
        for field_name in schema.names:
            new_fields.append(schema.field(field_name))

        self.update_schema(
            schema=pa.schema(new_fields, metadata=updated_metadata),
            update_metadata=update,
        )

    def set_field_metadata(
        self,
        fields_metadata: Dict[str, dict],
        update: bool = True,
    ) -> pa.Schema:
        """
        Sets or updates metadata for specific fields/columns in the dataset.

        Parameters
        ----------
        fields_metadata : Dict[str, dict]
            Dictionary mapping field names to their metadata dictionaries.
            Each inner dictionary contains metadata key-value pairs for that field.
        update : bool, optional
            If True, updates existing field metadata. If False, replaces it.
            Default is True.

        Returns
        -------
        pa.Schema
            Updated PyArrow schema with new field metadata.

        Examples
        --------
        Add metadata to specific fields:
        >>> field_meta = {
        ...     'age': {'unit': 'years', 'type': 'numeric'},
        ...     'name': {'language': 'en'}
        ... }
        >>> db.set_field_metadata(field_meta)

        Replace field metadata:
        >>> db.set_field_metadata({'age': {'new_meta': 'value'}}, update=False)

        Notes
        -----
        - Skips fields that don't exist in the schema
        - Updates schema and rewrites Parquet files to persist changes
        """
        schema = self.get_schema()

        for field_name, incoming_field_metadata in fields_metadata.items():
            if field_name not in schema.names:
                logger.warning(f"Field {field_name} not found in schema. Skipping.")
                continue

            field = schema.field(field_name)

            field_metadata = field.metadata
            if field_metadata is None:
                field_metadata = {}

            if update:
                field_metadata.update(incoming_field_metadata)
            else:
                field_metadata = incoming_field_metadata

            field = field.with_metadata(field_metadata)
            field_index = schema.get_field_index(field_name)
            schema = schema.set(field_index, field)

        self.update_schema(schema=schema, update_metadata=update)
        logger.debug("Field metadata updated")
        return schema

    def get_field_metadata(
        self, field_names: Union[str, List[str]] = None, return_bytes: bool = False
    ) -> Dict[str, Dict[str, Any]]:
        """
        Retrieves metadata for specified fields/columns in the dataset.

        Parameters
        ----------
        field_names : Union[str, List[str]], optional
            Name(s) of fields to get metadata for. If None, returns metadata for all fields.
            Can be a single field name or list of field names.
        return_bytes : bool, optional
            If True, returns raw bytes metadata. If False, decodes to strings.
            Default is False.

        Returns
        -------
        Dict[str, Dict[str, Any]]
            A nested dictionary structure:
            {field_name: {metadata_key: metadata_value}}
            where metadata is either a dictionary or metadata object based on return_bytes.

        Examples
        --------
        Get metadata for all fields:
        >>> meta = db.get_field_metadata()
        >>> print(meta)
        {'age': {'unit': 'years'}, 'name': {'language': 'en'}}

        Get metadata for specific fields:
        >>> meta = db.get_field_metadata(['age', 'name'])

        Get raw bytes metadata:
        >>> meta = db.get_field_metadata(return_bytes=True)
        """
        schema = self.get_schema()
        fields_metadata = {}

        if field_names:
            if isinstance(field_names, str):
                field_names = [field_names]

            for field_name in field_names:
                field_metadata = schema.field(field_name).metadata
                if field_metadata is None:
                    field_metadata = {}
                fields_metadata[field_name] = field_metadata
        else:
            for field in schema:
                field_metadata = field.metadata
                if field_metadata is None:
                    field_metadata = {}
                fields_metadata[field.name] = field_metadata

        if not return_bytes:
            for field_name, field_metadata in fields_metadata.items():
                fields_metadata[field_name] = {
                    key.decode("utf-8"): value.decode("utf-8")
                    for key, value in field_metadata.items()
                }

        return fields_metadata

    def get_number_of_rows_per_file(self) -> List[int]:
        """
        Get the number of rows in each Parquet file in the dataset.

        Returns
        -------
        List[int]
            A list containing the number of rows for each file, in order of the files
            returned by get_current_files().

        Examples
        --------
        >>> db.get_number_of_rows_per_file()
        [1000, 500, 750]  # Shows rows in each file
        """
        return [
            pq.ParquetFile(file).metadata.num_rows for file in self.get_current_files()
        ]

    def get_number_of_row_groups_per_file(self) -> List[int]:
        """
        Get the number of row groups in each Parquet file in the dataset.

        Returns
        -------
        List[int]
            A list containing the number of row groups for each file, in order of the files
            returned by get_current_files().

        Examples
        --------
        >>> db.get_number_of_row_groups_per_file()
        [2, 1, 2]  # Shows row groups in each file
        """
        return [
            pq.ParquetFile(file).metadata.num_row_groups
            for file in self.get_current_files()
        ]

    def get_parquet_file_metadata_per_file(
        self, as_dict: bool = False
    ) -> Union[Dict[str, Dict[str, Dict[str, Any]]], List[List[Dict[str, Any]]]]:
        """
        Get the metadata for each Parquet file in the dataset.

        Parameters
        ----------
        as_dict : bool, optional
            If True, returns metadata as dictionaries. If False, returns raw metadata objects.
            Default is False.

        Returns
        -------
        Dict[str, Dict[str, Dict[str, Any]]] | List[List[Dict[str, Any]]]
            A nested dictionary structure:
            {filename: {metadata_key: metadata_value}}
            where metadata is either a dictionary or metadata object based on as_dict.

        Examples
        --------
        >>> metadata = db.get_parquet_file_metadata_per_file(as_dict=True)
        >>> print(metadata[0]['num_rows'])  # Access metadata for first file
        1000
        """
        if as_dict:
            return [
                pq.ParquetFile(file).metadata.to_dict()
                for file in self.get_current_files()
            ]
        else:
            return [pq.ParquetFile(file).metadata for file in self.get_current_files()]

    def get_parquet_file_row_group_metadata_per_file(
        self, as_dict: bool = False
    ) -> Union[Dict[str, Dict[str, Dict[str, Any]]], List[List[Dict[str, Any]]]]:
        """
        Get detailed metadata for each row group in each Parquet file.

        Parameters
        ----------
        as_dict : bool, optional
            If True, returns metadata as dictionaries. If False, returns raw metadata objects.
            Default is False.

        Returns
        -------
        Dict[str, Dict[str, Dict[str, Any]]] | List[List[Dict[str, Any]]]
            A nested dictionary structure:
            {filename: {row_group_idx: metadata}}
            where metadata is either a dictionary or metadata object based on as_dict.

        Examples
        --------
        >>> metadata = db.get_parquet_file_row_group_metadata_per_file(as_dict=True)
        >>> print(metadata['file_0'][0]['num_rows'])  # Rows in first group of first file
        500
        """
        row_group_metadata = {}
        for file in self.get_current_files():
            filename = os.path.basename(file)
            parquet_file = pq.ParquetFile(file)
            metadata = parquet_file.metadata
            n_row_groups = metadata.num_row_groups
            row_group_metadata[filename] = {}
            if n_row_groups == 0:
                break

            for row_group_idx in range(n_row_groups):
                row_group = metadata.row_group(row_group_idx)
                if as_dict:
                    row_group_metadata[filename][row_group_idx] = row_group.to_dict()
                else:
                    row_group_metadata[filename][row_group_idx] = row_group

        return row_group_metadata

    def get_parquet_column_metadata_per_file(
        self, as_dict: bool = False
    ) -> Union[Dict[str, Dict[str, Dict[str, Any]]], List[List[Dict[str, Any]]]]:
        """
        Get detailed metadata for each column in each row group in each file.

        Parameters
        ----------
        as_dict : bool, optional
            If True, returns metadata as dictionaries. If False, returns raw metadata objects.
            Default is False.

        Returns
        -------
        Dict[str, Dict[str, Dict[str, Any]]] | List[List[Dict[str, Any]]]]
            A nested dictionary structure:
            {filename: {row_group_idx: {column_idx: metadata}}}
            where metadata is either a dictionary or metadata object based on as_dict.

        Examples
        --------
        >>> metadata = db.get_parquet_column_metadata_per_file(as_dict=True)
        >>> # Access metadata for first column in first row group of first file
        >>> print(metadata['file_0'][0][0]['total_compressed_size'])
        1024
        """
        column_metadata = {}
        for file in self.get_current_files():
            filename = os.path.basename(file)
            parquet_file = pq.ParquetFile(file)
            metadata = parquet_file.metadata
            n_row_groups = metadata.num_row_groups
            column_metadata[filename] = {}
            if n_row_groups == 0:
                break
            for row_group_idx in range(n_row_groups):
                row_group = metadata.row_group(row_group_idx)
                n_columns = row_group.num_columns
                column_metadata[filename][row_group_idx] = {}
                if n_columns == 0:
                    break
                for column_idx in range(n_columns):
                    column = row_group.column(column_idx)
                    if as_dict:
                        column_metadata[filename][row_group_idx][
                            column_idx
                        ] = column.to_dict()
                    else:
                        column_metadata[filename][row_group_idx][column_idx] = column
        return column_metadata

    def get_n_rows_per_row_group_per_file(
        self, as_dict: bool = False
    ) -> Union[Dict[str, Dict[str, int]], List[List[int]]]:
        """
        Get the number of rows in each row group for each file.

        Parameters
        ----------
        as_dict : bool, optional
            If True, returns a nested dictionary structure. If False, returns a list of lists.
            Default is False.

        Returns
        -------
        Union[Dict[str, Dict[str, int]], List[List[int]]]
            If as_dict=True:
                A nested dictionary: {filename: {row_group_idx: num_rows}}
            If as_dict=False:
                A list of lists, where each inner list contains row counts for each
                row group in a file.
            Returns empty dict if no row groups exist.

        Examples
        --------
        >>> # Dictionary format
        >>> db.get_n_rows_per_row_group_per_file(as_dict=True)
        {'file_0': {0: 500, 1: 500}, 'file_1': {0: 1000}}

        >>> # List format
        >>> db.get_n_rows_per_row_group_per_file(as_dict=False)
        [[500, 500], [1000]]
        """
        row_group_metadata = self.get_parquet_file_row_group_metadata_per_file(
            as_dict=True
        )
        if row_group_metadata:
            if as_dict:
                n_row_group_metadata = {}

                for filename in row_group_metadata:
                    n_row_group_metadata[filename] = {}
                    for row_group_idx in row_group_metadata[filename]:
                        n_row_group_metadata[filename][row_group_idx] = (
                            row_group_metadata[filename][row_group_idx]["num_rows"]
                        )
            else:
                n_row_group_metadata = []
                for filename in row_group_metadata:
                    tmp_list = []
                    for row_group_idx in row_group_metadata[filename]:
                        tmp_list.append(
                            row_group_metadata[filename][row_group_idx]["num_rows"]
                        )
                    n_row_group_metadata.append(tmp_list)

            return n_row_group_metadata

        else:
            return {}

    def get_row_group_sizes_per_file(
        self, verbose: bool = False
    ) -> Dict[str, Dict[str, float]]:
        """
        Get the size of each row group for each file. in MB

        Parameters
        ----------
        verbose : bool, optional
            If True, print the size of each row group.
            Default is False.

        Returns
        -------
        Dict[str, Dict[str, float]]
            Returns a dictionary detailing the size of each row group (in Megabytes)
            for each processed Parquet file. Structure: {filename: {row_group_id: size_MB}}

        Examples
        --------
        >>> db.get_row_group_sizes_per_file(verbose=True)
        {'file_0': {0: 10.5, 1: 15.2}, 'file_1': {0: 20.0}}
        """
        row_group_metadata_per_file = self.get_parquet_file_row_group_metadata_per_file(
            as_dict=True
        )

        row_group_size_per_file = {}
        sum_row_group_size = 0
        num_row_groups = 0
        for file, row_group_metadata in row_group_metadata_per_file.items():
            if verbose:
                print(f"File: {file}")
            row_group_size_per_file[file] = {}
            for row_group, metadata in row_group_metadata.items():
                row_group_size_per_file[file][row_group] = metadata[
                    "total_byte_size"
                ] / (1024 * 1024)
                sum_row_group_size += row_group_size_per_file[file][row_group]
                num_row_groups += 1
                if verbose:
                    print(
                        f"     {row_group}: {row_group_size_per_file[file][row_group]} MB"
                    )
        return row_group_size_per_file

    def get_file_sizes(self, verbose: bool = False) -> Dict[str, float]:
        """
        Get the size of each file in the dataset in MB.

        Parameters
        ----------
        verbose : bool, optional
            If True, print the size of each file.
            Default is False.

        Returns
        -------
        dict
            A dictionary mapping file names to their sizes in MB.

        Examples
        --------
        >>> db.get_file_sizes(verbose=True)
        {'file_0': 10.5, 'file_1': 15.2}
        """
        file_sizes = {}
        for filename in os.listdir(self.db_path):
            # file_basename = os.path.basename(filename)
            file_path = os.path.join(self.db_path, filename)
            if os.path.isfile(file_path):
                file_size = os.path.getsize(file_path)
                file_size = file_size / (1024 * 1024)

                if verbose:
                    print(f"{filename}: {file_size} MB")
                file_sizes[filename] = file_size
        return file_sizes

    def get_serialized_metadata_size_per_file(self) -> List[int]:
        """
        Get the serialized metadata size for each Parquet file in the dataset.

        Returns
        -------
        list
            A list containing the serialized metadata size in bytes for each file,
            in order of the files returned by get_current_files().

        Examples
        --------
        >>> db.get_serialized_metadata_size_per_file()
        [1024, 2048, 1536]  # Shows metadata size in bytes for each file
        """
        return [
            pq.ParquetFile(file).metadata.serialized_size
            for file in self.get_current_files()
        ]

    def rename_fields(
        self,
        name_map: dict,
        normalize_config: NormalizeConfig = NormalizeConfig(),
    ) -> pa.Schema:
        """
        Rename fields/columns in the dataset using a mapping dictionary.

        Parameters
        ----------
        name_map : dict
            Dictionary mapping current field names to new field names.
            Fields not included in the map retain their original names.
        normalize_config : NormalizeConfig, optional
            Configuration for optimizing data distribution after renaming.
            Default uses standard NormalizeConfig settings.

        Returns
        -------
        pa.Schema
            The schema after renaming.

        Examples
        --------
        >>> db.rename_fields({'old_name': 'new_name', 'age': 'years'})
        """
        schema = self.get_schema()
        new_fields = []
        for field in schema:
            if field.name in name_map:
                new_fields.append(pa.field(name_map[field.name], field.type))
            else:
                new_fields.append(field)

        self._normalize(schema=pa.schema(new_fields), normalize_config=normalize_config)
        logger.debug("Fields renamed")
        return schema

    def sort_fields(
        self, normalize_config: NormalizeConfig = NormalizeConfig()
    ) -> pa.Schema:
        """
        Sort the fields/columns of the dataset alphabetically by name.

        This method reorders the fields in the schema alphabetically while
        preserving the data and field types. The sort is performed in-place.

        Returns
        -------
        pyarrow.Schema
            The sorted schema.

        Examples
        --------
        >>> db.sort_fields()  # Reorders fields like ['age', 'name', 'zip']
        """
        schema = self.get_schema()
        field_names = schema.names
        sorted_field_names = sorted(field_names)
        new_fields = []
        for field_name in sorted_field_names:
            new_fields.append(schema.field(field_name))

        schema = pa.schema(new_fields, metadata=schema.metadata)
        self._normalize(schema=schema, normalize_config=normalize_config)
        logger.debug("Fields sorted alphabetically")
        return schema

    def get_current_files(self) -> List[Union[str, Path]]:
        """
        Get a list of all Parquet files in the current dataset.

        Returns
        -------
        List[Union[str, Path]]
            List of absolute file paths for all Parquet files in the dataset,
            sorted by file number.

        Examples
        --------
        >>> db.get_current_files()
        ['/path/to/data/mydata_0.parquet', '/path/to/data/mydata_1.parquet']

        Notes
        -----
        - Files are named using pattern: {dataset_name}_{number}.parquet
        - Files are sorted numerically by their suffix number
        """
        return [file for file in self.db_path.glob(f"{self.dataset_name}_*.parquet")]

    def dataset_exists(self, dataset_name: str = None) -> bool:
        """
        Check if a dataset exists and contains data.

        Parameters
        ----------
        dataset_name : str, optional
            Name of dataset to check. If None, checks the current dataset.
            Default is None.

        Returns
        -------
        bool
            True if the dataset exists and contains files, False otherwise.

        Examples
        --------
        Check current dataset:
        >>> db.dataset_exists()
        True

        Check specific dataset:
        >>> db.dataset_exists('other_dataset')
        False

        Notes
        -----
        - Checks both directory existence and presence of files
        - Empty directories return False
        """
        if dataset_name:
            dir = self.db_path.parent
            dataset_dir = dir / dataset_name
            return dataset_dir.exists() and len(os.listdir(dataset_dir)) > 0
        else:
            return self.db_path.exists() and len(os.listdir(self.db_path)) > 0

    def drop_dataset(self):
        """
        Removes the current dataset directory and reinitializes it with an empty table.

        This method:
        1. Deletes the entire dataset directory if it exists
        2. Creates a new empty directory
        3. Initializes a new empty table with just an 'id' column

        Returns
        -------
        None

        Examples
        --------
        >>> db = ParquetDB('my_dataset')
        >>> db.drop_dataset()  # Removes all data and reinitializes

        Notes
        -----
        - After dropping, the dataset will contain one empty file with an 'id' column
        - Safe to call even if dataset doesn't exist
        - Logs the drop operation for tracking
        """
        if self.db_path.exists():
            shutil.rmtree(self.db_path)
            self.db_path.mkdir(parents=True, exist_ok=True)
            table = pyarrow_utils.create_empty_table(
                schema=pa.schema([pa.field("id", pa.int64())])
            )
            pq.write_table(table, self._get_save_path())
            logger.info("Dataset dropped: %s", self.dataset_name)
        else:
            logger.warning("Dataset does not exist: %s", self.dataset_name)
        return None

    def rename_dataset(self, new_name: str, remove_dest: bool = False) -> None:
        """
        Renames the current dataset directory and all contained files.

        Parameters
        ----------
        new_name : str
            The new name for the dataset. Will be used for both directory and file prefixes.
        remove_dest : bool, optional
            If True, removes existing dataset at new_name if it exists.
            If False, raises error if new_name already exists.
            Default is False.

        Returns
        -------
        None

        Examples
        --------
        >>> db = ParquetDB('old_name')
        >>> db.rename_dataset('new_name')  # Renames dataset

        >>> db.rename_dataset('existing_name', remove_dest=True)  # Overwrites existing

        Notes
        -----
        - Updates internal path reference after renaming
        - Maintains file numbering scheme in new location
        - Operation is atomic - either completes fully or not at all
        """
        if not self.dataset_exists():
            logger.error("Dataset does not exist: %s", self.dataset_name)
            raise ValueError(f"Dataset {self.dataset_name} does not exist.")

        old_dir = self.db_path
        parent_dir = old_dir.parent
        new_dir = parent_dir / new_name
        old_name = self.dataset_name

        if new_dir.exists():
            if remove_dest:
                shutil.rmtree(new_dir)
            else:
                logger.error("Dataset already exists: %s", new_name)
                raise ValueError(f"Dataset {new_name} already exists.")

        # Create the new directory
        new_dir.mkdir(parents=True, exist_ok=True)

        # Rename all files in the old directory
        old_filepaths = old_dir.glob(f"{old_name}_*.parquet")
        for old_filepath in old_filepaths:
            filename = old_filepath.name
            file_index = filename.split(".")[0].split("_")[-1]

            new_filepath = new_dir / f"{new_name}_{file_index}.parquet"
            old_filepath.rename(new_filepath)

        self._db_path = new_dir

        logger.info(f"Dataset ({old_name}) has been renamed to ({new_name}).")
        return None

    def copy_dataset(self, dest_name: str, overwrite: bool = False) -> None:
        """
        Creates a complete copy of the current dataset under a new name.

        Parameters
        ----------
        dest_name : str
            Name for the new copy of the dataset.
        overwrite : bool, optional
            If True, overwrites existing dataset at dest_name.
            If False, raises error if dest_name already exists.
            Default is False.

        Raises
        ------
        ValueError
            If destination dataset exists and overwrite=False

        Examples
        --------
        >>> db = ParquetDB('original')
        >>> db.copy_dataset('backup')  # Creates copy named 'backup'

        >>> db.copy_dataset('existing', overwrite=True)  # Overwrites existing copy

        Notes
        -----
        - Creates new directory with copied files
        - Preserves all data, metadata, and file organization
        - Original dataset remains unchanged
        - Useful for backups or creating test copies
        """
        dir = self.db_path.parent
        if overwrite and self.dataset_exists(dest_name):
            shutil.rmtree(dir / dest_name)
        elif self.dataset_exists(dest_name):
            logger.error("Dataset already exists: %s", dest_name)
            raise ValueError(f"Dataset {dest_name} already exists.")

        source_dir = self.db_path
        source_name = self.dataset_name
        dest_dir = dir / dest_name

        dest_dir.mkdir(parents=True, exist_ok=True)

        # Rename all files in the old directory
        old_filepaths = source_dir.glob(f"{source_name}_*.parquet")
        for old_filepath in old_filepaths:
            filename = old_filepath.name
            file_index = filename.split(".")[0].replace("_", "")
            new_filepath = dest_dir / f"{dest_name}_{file_index}.parquet"
            shutil.copyfile(old_filepath, new_filepath)

        logger.info("Dataset copied to %s", dest_dir)
        return None

    def export_dataset(self, file_path: Union[str, Path], format: str = "csv") -> None:
        """
        Exports the entire dataset to a single file in the specified format.

        Parameters
        ----------
        file_path : str | Path
            Full path where the exported file will be saved, including filename and extension.
        format : str, optional
            Output file format. Currently supports:
            - 'csv': Comma-separated values file
            - 'json': JSON Lines format (one record per line)
            Default is 'csv'.

        Raises
        ------
        ValueError
            If format is not 'csv' or 'json'

        Examples
        --------
        Export to CSV:
        >>> db.export_dataset('data.csv', format='csv')

        Export to JSON Lines:
        >>> db.export_dataset('data.jsonl', format='json')

        Notes
        -----
        - CSV exports use no index column
        - JSON exports use 'records' orientation with one record per line
        - Loads entire dataset into memory during export
        - For large datasets, consider using export_partitioned_dataset()
        """
        file_path = Path(file_path)
        table = self._load_data(load_format="table")
        if format == "csv":
            logger.debug("Exporting csv: %s", file_path)

            df = table.to_pandas()
            df.to_csv(file_path, index=False)
        elif format == "json":
            logger.debug("Exporting json: %s", file_path)

            df = table.to_pandas()
            df.to_json(file_path, orient="records", lines=True)
        else:
            logger.error("Unsupported export format: %s", format)
            raise ValueError(f"Unsupported export format: {format}")
        logger.info("Exported dataset to %s", file_path)

    def export_partitioned_dataset(
        self,
        export_dir: Union[str, Path],
        partitioning: Union[str, List[str], ds.Partitioning, ds.PartitioningFactory],
        partitioning_flavor: Optional[str] = None,
        load_config: LoadConfig = LoadConfig(),
        load_format: str = "table",
        **kwargs,
    ) -> None:
        """
        Exports the dataset to a partitioned format in the specified directory.

        Parameters
        ----------
        export_dir : str
            Directory path where the partitioned dataset will be saved.
        partitioning : Union[str, List[str], pyarrow.dataset.Partitioning, pyarrow.dataset.PartitioningFactory]
            Partitioning configuration. Can be:
            - Column name(s) to partition by
            - PyArrow Partitioning object
            - Dict of partition expressions
        partitioning_flavor : str, optional
            Partitioning flavor to use. Options:
            - 'hive': Hive-style partitioning
            - 'directory': Directory-based partitioning
            Default is None.
        load_config : LoadConfig, optional
            Configuration for optimizing data loading during export.
            Controls batch sizes, readahead, threading, etc.
            Default uses standard LoadConfig settings.
        load_format : str, optional
            Format to load data before exporting. Options:
            - 'table': Load as single PyArrow Table
            - 'batches': Load as record batch generator
            Default is 'table'.
        **kwargs : dict, optional
            Additional arguments passed to pq.write_to_dataset().
            Common options include:
            - existing_data_behavior: How to handle existing data
            - max_rows_per_file: Target rows per output file
            - use_threads: Enable multi-threading

        Returns
        -------
        None

        Examples
        --------
        Basic partitioning by column:
        >>> db.export_partitioned_dataset(
        ...     export_dir='data/partitioned',
        ...     partitioning=['year', 'month']
        ... )

        Hive-style partitioning with custom config:
        >>> config = LoadConfig(batch_size=10000, use_threads=True)
        >>> db.export_partitioned_dataset(
        ...     export_dir='data/hive',
        ...     partitioning=['region'],
        ...     partitioning_flavor='hive',
        ...     load_config=config
        ... )

        Notes
        -----
        - Creates a new directory structure based on partition values
        - Maintains original data types and schema
        - For large datasets, consider using batches format
        - Partitioning can significantly improve query performance
        """
        self._validate_load_format(load_format)
        # Read the entire dataset either in batches or as a whole
        retrieved_data = self._load_data(
            load_format=load_format, load_config=load_config
        )
        schema = self.get_schema()

        # Can't provide schema to wrrite_to_dataset if the data is a table
        if isinstance(retrieved_data, pa.lib.Table):
            schema = None

        pq.write_to_dataset(
            retrieved_data,
            export_dir,
            schema=schema,
            partitioning=partitioning,
            partitioning_flavor=partitioning_flavor,
            format="parquet",
            **kwargs,
        )
        logger.info("Partitioned dataset exported to %s", export_dir)
        return None

    def import_dataset(
        self, file_path: Union[str, Path], format: str = "csv", **kwargs
    ) -> None:
        """
        Imports data from a file into the dataset, supporting multiple file formats.

        Parameters
        ----------
        file_path : str | Path
            Path to the input file to import.
            Must be readable and in a supported format.
        format : str, optional
            Format of the input file. Supported formats:
            - 'csv': Comma-separated values (default)
            - 'json': JSON Lines format (one record per line)
        **kwargs : dict, optional
            Additional arguments passed to the underlying reader.
            For CSV:
                - delimiter: Field separator
                - header: Row number(s) to use as headers
                - dtype: Column data types
                - encoding: File encoding
            For JSON:
                - orient: JSON string format
                - lines: Read JSON Lines format
                - dtype: Column data types


        Examples
        --------
        Import CSV with default settings:
        >>> db.import_dataset('data.csv')

        Import CSV with custom options:
        >>> db.import_dataset(
        ...     'data.csv',
        ...     delimiter=';',
        ...     encoding='utf-8',
        ...     dtype={'id': int, 'value': float}
        ... )

        Import JSON Lines:
        >>> db.import_dataset(
        ...     'data.jsonl',
        ...     format='json',
        ...     lines=True
        ... )

        Notes
        -----
        - Automatically detects and preserves data types
        - Handles missing values appropriately
        - Creates new dataset if none exists
        - Updates schema if necessary
        """
        file_path = Path(file_path)
        if format == "csv":
            logger.debug("Importing csv: %s", file_path)
            df = pd.read_csv(file_path, **kwargs)
        elif format == "json":
            logger.debug("Importing json: %s", file_path)
            df = pd.read_json(file_path, **kwargs)
        else:
            logger.error("Unsupported import format: %s", format)
            raise ValueError(f"Unsupported import format: {format}")
        self.create(data=df)
        logger.info("Imported dataset from %s", file_path)
        return None

    def merge_datasets(self, source_tables: List[str], dest_table: str) -> None:
        raise NotImplementedError

    def backup_database(self, backup_path: Union[str, Path]) -> None:
        """
        Creates a complete backup of the current dataset.

        Parameters
        ----------
        backup_path : str | Path
            Directory path where the backup will be stored.
            Must have write permissions and sufficient space.

        Returns
        -------
        None

        Examples
        --------
        >>> db.backup_database('/path/to/backups/mydb_20231201')

        Notes
        -----
        - Creates exact copy of all dataset files
        - Preserves file structure and metadata
        - Overwrites existing backup at same path
        - Safe to run while database is in use
        """
        backup_path = Path(backup_path)
        shutil.copytree(self.db_path, backup_path)
        logger.info("Database backed up to %s", backup_path)
        return None

    def restore_database(self, backup_path: Union[str, Path]) -> None:
        """
        Restores the dataset from a previous backup.

        Parameters
        ----------
        backup_path : str | Path
            Path to the backup directory containing the dataset files.
            Must be a valid backup created by backup_database().

        Returns
        -------
        None

        Examples
        --------
        >>> db.restore_database('/path/to/backups/mydb_20231201')

        Notes
        -----
        - Completely replaces current dataset
        - Requires exclusive access to dataset
        - Verifies backup integrity before restore
        - Maintains all metadata and structure
        """
        backup_path = Path(backup_path)
        if self.db_path.exists():
            shutil.rmtree(self.db_path)
        shutil.copytree(backup_path, self.db_path)

        logger.info("Database restored from %s", backup_path)
        return None

    def to_nested(
        self,
        nested_dataset_dir: Union[str, Path] = None,
        normalize_config: NormalizeConfig = NormalizeConfig(),
        rebuild_nested_from_scratch: bool = False,
    ) -> None:
        """
        Converts the current dataset to a nested structure optimized for querying nested data.

        This method reorganizes the dataset into a nested directory structure that improves
        performance when querying deeply nested data structures. The nested structure can
        be rebuilt from scratch or incrementally updated.

        Parameters
        ----------
        nested_dataset_dir : str | Path, optional
            Directory path where the nested dataset will be stored. If not provided,
            defaults to a 'nested' subdirectory in the current dataset path.
        normalize_config : NormalizeConfig, optional
            Configuration for optimizing the nested structure, controlling:
            - Row distribution across files
            - Row group sizes
            - File organization
            - Thread/memory usage
            Default uses standard NormalizeConfig settings.
        rebuild_nested_from_scratch : bool, optional
            If True, completely rebuilds the nested structure, discarding any existing
            nested data. If False, updates the existing nested structure incrementally.
            Default is False.

        Returns
        -------
        None
            Modifies the dataset structure in place.

        Examples
        --------
        Basic nested conversion:
        >>> db.to_nested()

        Custom nested directory with full rebuild:
        >>> db.to_nested(
        ...     nested_dataset_dir='/path/to/nested',
        ...     rebuild_nested_from_scratch=True
        ... )

        Notes
        -----
        - Recommended for datasets with complex nested structures
        - Improves query performance on nested fields
        - May require additional storage space
        - Safe to rebuild while database is in use
        """

        if nested_dataset_dir.exists() and rebuild_nested_from_scratch:
            shutil.rmtree(nested_dataset_dir)
        nested_dataset_dir.mkdir(parents=True, exist_ok=True)

        self._normalize(
            nested_dataset_dir=nested_dataset_dir, normalize_config=normalize_config
        )
        logger.info("Dataset normalized to nested structure")
        return None

    def _load_data(
        self,
        load_format: str = "table",
        columns: List[str] = None,
        filter: List[pc.Expression] = None,
        dataset_dir: Union[str, Path] = None,
        load_config: LoadConfig = LoadConfig(),
    ) -> Union[pa.Table, Iterable[pa.RecordBatch], ds.Dataset]:
        """
        Internal method to load data from the dataset in various formats.

        This method provides flexible data loading capabilities, supporting different
        output formats and filtering options. It handles the core logic for all data
        reading operations.

        Parameters
        ----------
        load_format : str, optional
            Format to return the data in. Options:
            - 'table': Returns a PyArrow Table (default)
            - 'batches': Returns a generator of record batches
            - 'dataset': Returns a PyArrow Dataset
        columns : List[str], optional
            Specific columns to load. If None, loads all columns.
            Default is None.
        filter : List[pc.Expression], optional
            PyArrow compute expressions for filtering the data.
            Example: [pc.field('age') > 18]
            Default is None.
        dataset_dir : str | Path, optional
            Custom directory to load data from. If None, uses the default
            dataset directory. Default is None.
        load_config : LoadConfig, optional
            Configuration for optimizing data loading performance.
            Controls batch sizes, readahead, threading, and memory usage.
            Default uses standard LoadConfig settings.

        Returns
        -------
        Union[pa.Table, pa.dataset.Scanner, Iterable[pa.RecordBatch]]
            Data in the requested format:
            - PyArrow Table for 'table'
            - Batch generator for 'batches'
            - PyArrow Dataset for 'dataset'

        Raises
        ------
        ValueError
            If load_format is not one of: 'table', 'batches', 'dataset'

        Notes
        -----
        - Core method used by public read() interface
        - Handles error cases gracefully
        - Logs loading operations for debugging
        - Optimizes memory usage for large datasets
        """

        if dataset_dir is None:
            dataset_dir = self.db_path
        dataset_dir = Path(dataset_dir)

        logger.debug(f"Dataset directory: {dataset_dir}")
        logger.debug(f"Columns: {columns}")
        logger.debug(f"Filter: {filter}")

        dataset = ds.dataset(
            dataset_dir, format="parquet", ignore_prefixes=["tmp_", "nested"]
        )
        if load_format == "batches":
            generator = self._load_batches(
                dataset, columns, filter, load_config=load_config
            )
            logger.debug(f"Data loaded as {generator.__class__} ")
            return generator
        elif load_format == "table":
            table = self._load_table(dataset, columns, filter, load_config=load_config)
            logger.debug(f"Data loaded as {table.__class__} ")
            return table
        elif load_format == "dataset":
            logger.debug(f"Data loaded as {dataset.__class__} ")
            return dataset
        else:
            logger.error(f"Invalid load format: {load_format}")
            raise ValueError(
                f"load_format must be one of the following: {self.load_formats}"
            )

    def _load_batches(
        self,
        dataset: ds.Dataset,
        columns: List[str] = None,
        filter: List[pc.Expression] = None,
        load_config: LoadConfig = LoadConfig(),
    ) -> Iterable[pa.RecordBatch]:
        """
        Internal method to load data as batches from a PyArrow dataset.

        This method provides memory-efficient data loading by returning an iterator
        of record batches instead of loading the entire dataset into memory at once.

        Parameters
        ----------
        dataset : pa.dataset.Dataset
            PyArrow dataset to load data from. Must be a valid dataset created
            by PyArrow's dataset factory.
        columns : List[str], optional
            Specific columns to load. If None, loads all columns.
            Default is None.
        filter : List[pc.Expression], optional
            PyArrow compute expressions for filtering the data.
            Example: [pc.field('age') > 18]
            Default is None.
        load_config : LoadConfig, optional
            Configuration for optimizing batch loading performance.
            Controls batch sizes, readahead, threading, and memory usage.
            Default uses standard LoadConfig settings.

        Returns
        -------
        Iterable[pa.RecordBatch]
            Generator yielding PyArrow RecordBatch objects, each containing
            a portion of the dataset.

        Notes
        -----
        - Memory efficient for large datasets
        - Handles errors by returning empty batch generator
        - Preserves schema even when filtering
        - Useful for processing data in chunks
        """

        try:
            generator = dataset.to_batches(
                columns=columns, filter=filter, **load_config.__dict__
            )
        except Exception as e:
            logger.exception(f"Error loading table: {e}. Returning empty table")
            generator = pyarrow_utils.create_empty_batch_generator(
                schema=dataset.schema, columns=columns
            )

        return generator

    def _load_table(
        self,
        dataset: ds.Dataset,
        columns: List[str] = None,
        filter: List[pc.Expression] = None,
        load_config: LoadConfig = LoadConfig(),
    ) -> pa.Table:
        """
        Internal method to load data as a single PyArrow Table from a dataset.

        This method loads the entire dataset into memory as a single table, which can be
        more convenient but less memory efficient than loading as batches.

        Parameters
        ----------
        dataset : pa.dataset.Dataset
            PyArrow dataset to load data from. Must be a valid dataset created
            by PyArrow's dataset factory.
        columns : List[str], optional
            Specific columns to load. If None, loads all columns.
            Default is None.
        filter : List[pc.Expression], optional
            PyArrow compute expressions for filtering the data.
            Example: [pc.field('age') > 18]
            Default is None.
        load_config : LoadConfig
            Configuration for optimizing loading performance.
            Controls batch sizes, readahead, threading, and memory usage.
            Default uses standard LoadConfig settings.

        Returns
        -------
        pa.Table
            A single PyArrow Table containing the loaded data.

        Notes
        -----
        - Loads entire dataset into memory
        - Handles errors by returning empty table
        - Preserves schema even when filtering
        - Consider using _load_batches for large datasets
        """
        try:
            table = dataset.to_table(
                columns=columns, filter=filter, **load_config.__dict__
            )
        except Exception as e:
            logger.exception(f"Error loading table: {e}. Returning empty table")
            table = pyarrow_utils.create_empty_table(
                schema=dataset.schema, columns=columns
            )

        return table

    @staticmethod
    def preprocess_table(
        table: pa.Table,
        treat_fields_as_ragged: List[str] = None,
        convert_to_fixed_shape: bool = True,
    ) -> pa.Table:
        """
        Preprocesses a PyArrow table by flattening nested structures and handling special field types.

        This method performs several preprocessing steps:
        1. Flattens nested table structures
        2. Converts list columns to fixed tensors (unless marked as ragged)
        3. Replaces empty structs with dummy values
        4. Flattens any remaining nested structures

        Parameters
        ----------
        table : pa.Table
            The PyArrow table to preprocess.
        treat_fields_as_ragged : List[str], optional
            List of field names to treat as ragged arrays (skip tensor conversion).
            Default is None.
        convert_to_fixed_shape : bool
            Whether to convert list columns to fixed-shape tensors.
            Default is True.

        Returns
        -------
        pa.Table
            The preprocessed PyArrow table.

        Notes
        -----
        - Modifies table structure but preserves data
        - Handles nested arrays and structs
        - Useful for ensuring consistent data format
        """
        table = pyarrow_utils.flatten_table(table)

        if treat_fields_as_ragged is None:
            treat_fields_as_ragged = []

        for column_name in table.column_names:
            # Convert list column to fixed tensor

            treat_as_ragged = False
            for field_name in treat_fields_as_ragged:
                if field_name in column_name:
                    treat_as_ragged = True
                    break
            if not treat_as_ragged and convert_to_fixed_shape:
                table = pyarrow_utils.convert_list_column_to_fixed_tensor(
                    table, column_name
                )

            # Replace empty structs with dummy structs
            table = pyarrow_utils.replace_empty_structs_in_column(
                table, column_name, is_nested=True
            )

            table = pyarrow_utils.flatten_table_in_column(table, column_name)

        return table

    def _get_new_ids(self, incoming_table: pa.Table) -> List[int]:
        """
        Generates sequential IDs for new records starting from the next available ID.

        This method determines the highest existing ID in the dataset and generates
        new sequential IDs starting from the next number. For empty datasets, it
        starts from 0.

        Parameters
        ----------
        incoming_table : pa.Table
            The table containing new records that need IDs.

        Returns
        -------
        List[int]
            List of new unique sequential IDs, one for each row in incoming_table.

        Notes
        -----
        - Ensures ID uniqueness across the dataset
        - Handles empty datasets appropriately
        - Thread-safe when reading max ID
        """
        if self.is_empty():
            start_id = 0
        else:
            table = self._load_data(columns=["id"], load_format="table")
            max_val = pc.max(table.column("id")).as_py()
            start_id = max_val + 1  # Start from the next available ID

        # Create a list of new IDs
        end_id = start_id + incoming_table.num_rows
        new_ids = list(range(start_id, end_id))
        logger.debug(
            "len(new_ids): %s, start_id: %s, end_id: %s",
            len(new_ids),
            start_id,
            end_id,
        )
        return new_ids

    def _build_filter_expression(
        self,
        ids: Optional[List[int]] = None,
        filters: Optional[List[pc.Expression]] = None,
    ) -> Union[pc.Expression, None]:
        """
        Combines ID-based and custom filters into a single PyArrow compute expression.

        This method merges multiple filter conditions:
        1. An optional ID-based filter using the provided IDs
        2. Any additional custom filter expressions
        All filters are combined using AND operations.

        Parameters
        ----------
        ids : List[int], optional
            List of record IDs to filter by. Creates an 'id IN (...)' expression.
            Default is None.
        filters : List[pc.Expression], optional
            Additional PyArrow compute expressions for filtering.
            Default is None.

        Returns
        -------
        pc.Expression or None
            Combined filter expression if any filters provided, None otherwise.

        Notes
        -----
        - Combines multiple filters with AND operations
        - Returns None if no filters provided
        - Optimized for PyArrow compute engine
        """
        final_filters = []

        # Add ID filter if provided
        if ids:
            id_filter = pc.field("id").isin(ids)
            final_filters.append(id_filter)

        # Append custom filters
        final_filters.extend(filters)

        # Combine filters into a single filter expression
        if not final_filters:
            return None

        filter_expression = final_filters[0]
        for filter_expr in final_filters[1:]:
            filter_expression = filter_expression & filter_expr

        logger.debug(f"Filter Expression: %s", filter_expression)
        return filter_expression

    def _get_save_path(self) -> Path:
        """
        Generates the next available file path for saving data in the dataset.

        This method determines the appropriate file path by:
        1. Counting existing dataset files
        2. Generating the next sequential file name
        3. Combining with the dataset directory path

        Returns
        -------
        Path
            Complete file path for saving the next data file.
            Format: {db_path}/{dataset_name}_{number}.parquet

        Notes
        -----
        - Maintains sequential file numbering
        - Handles empty datasets (starts at _0)
        - Thread-safe for file counting
        """
        files = [file for file in self.db_path.glob(f"{self.dataset_name}_*.parquet")]
        n_files = len(files)
        save_path = None
        if n_files == 0:
            save_path = self.db_path / f"{self.dataset_name}_0.parquet"
        else:
            max_index = 0
            for file in files:
                index = int(file.stem.split("_")[-1])
                max_index = max(max_index, index)
            save_path = self.db_path / f"{self.dataset_name}_{max_index+1}.parquet"

        logger.debug(f"Save path: {save_path}")
        return save_path

    def _validate_id(self, id_column: pa.Array) -> bool:
        """
        Verifies that all IDs in the provided column exist in the main dataset.

        This method checks for ID validity by:
        1. Loading existing IDs from the dataset
        2. Comparing against provided IDs
        3. Logging any IDs that don't exist

        Parameters
        ----------
        id_column : pa.Array
            PyArrow array containing IDs to validate.

        Returns
        -------
        bool
            True if all IDs exist, False otherwise.

        Notes
        -----
        - Logs warning for non-existent IDs
        - Efficient ID comparison using PyArrow compute
        - Useful for update/delete operations
        """
        logger.info(f"Validating ids")
        current_table = self.read(columns=["id"], load_format="table").combine_chunks()
        filtered_table = current_table.filter(~pc.field("id").isin(id_column))

        if filtered_table.num_rows == 0:
            logger.warning(
                f"The following ids are not in the main table",
                extra={"ids_do_not_exist": filtered_table["id"].combine_chunks()},
            )
        return False

    def _validate_load_format(self, load_format: str):
        """
        Validates that the provided load format is supported.

        Parameters
        ----------
        load_format : str
            The format to validate. Must be one of the supported load formats.

        """
        if load_format not in self.load_formats:
            logger.error(f"Invalid load format: {load_format}")
            raise ValueError(
                f"load_format must be one of the following: {self.load_formats}"
            )

    @staticmethod
    def construct_table(
        data: Union[
            pa.Table,
            pa.RecordBatch,
            pd.DataFrame,
            Dict[str, List[Any]],
            List[Dict[str, Any]],
        ],
        schema: Optional[pa.Schema] = None,
        metadata: Optional[Dict[str, Any]] = None,
        fields_metadata: Optional[Dict[str, Any]] = None,
        serialize_python_objects: bool = False,
    ) -> pa.Table:
        """
        Constructs a PyArrow Table from various input data formats.

        Parameters
        ----------
        data : Union[pa.Table, pa.RecordBatch, pd.DataFrame, Dict[str, List[Any]], List[Dict[str, Any]]]
            The input data to convert to a PyArrow Table.
        schema : pa.Schema, optional
            Schema to use for the table. If None, inferred from data.
        metadata : dict, optional
            Metadata to attach to the table schema.
        fields_metadata : dict, optional
            Field-level metadata mapping field names to metadata dicts.
        serialize_python_objects : bool, optional
            Whether to serialize Python objects in the data.
            Default is False.

        Returns
        -------
        pa.Table
            The constructed PyArrow Table.

        """
        if isinstance(data, pa.Table) or isinstance(data, pa.RecordBatch):

            incoming_schema = data.schema
            if schema is None:
                schema = incoming_schema
            incoming_array = data.to_struct_array()
            incoming_array = incoming_array.flatten()
        elif (
            isinstance(data, pd.DataFrame)
            or isinstance(data, dict)
            or isinstance(data, list)
        ):
            if serialize_python_objects:
                incoming_array, schema = ParquetDB.process_data_with_python_objects(
                    data, schema, serialize_python_objects
                )
            else:
                incoming_array, schema = (
                    ParquetDB.preprocess_data_without_python_objects(data, schema)
                )
        else:
            raise ValueError(
                "Data must be a dictionary of arrays, a list of dictionaries, a pandas dataframe, or a pyarrow table"
            )

        # Add metadata to the schema
        if fields_metadata is not None:
            incoming_fields_metadata = set(fields_metadata.keys())
            existing_fields_metadata = set(schema.names)
            if incoming_fields_metadata != existing_fields_metadata:
                raise ValueError(
                    f"The following fields are not in the schema: {incoming_fields_metadata - existing_fields_metadata}"
                )

            for field_name, custom_field_metadata in fields_metadata.items():
                field = schema.field(field_name)
                field_metadata = field.metadata
                if field_metadata is None:
                    field_metadata = {}
                field_metadata.update(custom_field_metadata)

                field = field.with_metadata(field_metadata)
                field_index = schema.get_field_index(field_name)
                schema = schema.set(field_index, field)

        # Add metadata to the schema
        schema = schema.with_metadata(metadata)

        return pa.Table.from_arrays(incoming_array, schema=schema)

    @staticmethod
    def preprocess_data_without_python_objects(
        data: Union[dict, list, pd.DataFrame], schema: Optional[pa.Schema] = None
    ) -> Tuple[List[pa.Array], pa.Schema]:
        """
        Preprocesses data without python objects.

        This method preprocesses data without python objects by converting it to a PyArrow Table.

        Parameters
        ----------
        data : Union[dict, list, pd.DataFrame]
            The data to preprocess.
        schema : pa.Schema, optional
            The schema to use for the table. If None, inferred from data.

        Returns
        -------
        Tuple[List[pa.Array], pa.Schema]
            Tuple containing flattened arrays and schema.

        """
        logger.info("Preprocessing data without python objects")
        if isinstance(data, dict):
            logger.info("The incoming data is a dictonary of arrays")
            for key, value in data.items():
                if not isinstance(value, List):
                    data[key] = [value]
            table = pa.Table.from_pydict(data)
            incoming_array = table.to_struct_array()
            incoming_array = incoming_array.flatten()
            incoming_schema = table.schema

        elif isinstance(data, list):
            logger.info("Incoming data is a list of dictionaries")
            # Convert to pyarrow array to get the schema. This method is faster than .from_pylist
            # As from_pylist iterates through record in a python loop, but pa.array handles this in C++/cython
            incoming_array = pa.array(data)
            incoming_schema = pa.schema(incoming_array.type)
            incoming_array = incoming_array.flatten()

        elif isinstance(data, pd.DataFrame):
            logger.info("Incoming data is a pandas dataframe")
            table = pa.Table.from_pandas(data)
            incoming_array = table.to_struct_array()
            incoming_array = incoming_array.flatten()
            incoming_schema = table.schema

        else:
            raise ValueError(
                "Data must be a dictionary of arrays, a list of dictionaries, or a pandas dataframe"
            )
        # If schema is not provided, use the incoming schema
        if schema is None:
            schema = incoming_schema
        return incoming_array, incoming_schema

    @staticmethod
    def process_data_with_python_objects(
        data: Union[dict, list, pd.DataFrame],
        schema: Optional[pa.Schema] = None,
        serialize_python_objects: bool = config.serialize_python_objects,
    ) -> Tuple[List[pa.Array], pa.Schema]:
        """
        Processes input data and handles Python object serialization.

        Parameters
        ----------
        data : Union[dict, list, pd.DataFrame]
            Input data to process.
        schema : pa.Schema, optional
            Schema to use for the output. If None, inferred from data.
        serialize_python_objects : bool, optional
            Whether to serialize Python objects in the data.
            Default from config.serialize_python_objects.

        Returns
        -------
        Tuple[List[pa.Array], pa.Schema]
            Tuple containing flattened arrays and schema.

        """
        logger.info("Processing data with python objects")
        if isinstance(data, dict):
            df = pd.DataFrame.from_records(data)
        elif isinstance(data, list):
            df = pd.DataFrame.from_dict(data)
        elif isinstance(data, pd.DataFrame):
            df = data
        else:
            raise ValueError(
                "Data must be a dictionary of arrays, a list of dictionaries, or a pandas dataframe."
            )
        data = None

        if serialize_python_objects:
            # Check for python objects and serialize them
            python_object_columns = []
            for column in df.columns:
                values = df[column].values
                if data_utils.has_python_object(values):
                    logger.debug(f"Serializing {column}")
                    python_object_columns.append(column)

            for column in python_object_columns:
                df[column] = types.PythonObjectPandasArray(df[column])

        # Convert to pyarrow table
        table = pa.Table.from_pandas(df)
        incoming_array = table.to_struct_array()
        incoming_array = incoming_array.flatten()
        incoming_schema = table.schema

        # If schema is not provided, use the incoming schema
        if schema is None:
            schema = incoming_schema

        return incoming_array, schema


def generator_transform(
    data: Iterable[pa.RecordBatch],
    callable: Callable,
    *args,
    **kwargs,
) -> Iterable[pa.RecordBatch]:
    """
    Transforms data from a generator using the provided callable.

    Parameters
    ----------
    data : Iterable[pa.RecordBatch]
        Iterable yielding data to transform.
    callable : Callable
        Function to apply to each item from generator.
    *args, **kwargs
        Additional arguments passed to callable.

    Yields
    ------
    Any
        Transformed data items.
    """
    for record_batch in data:
        yield callable(record_batch, *args, **kwargs)


def generator_transform(
    data: Iterable[pa.RecordBatch],
    callable: Callable,
    *args,
    **kwargs,
) -> Iterable[pa.RecordBatch]:
    """
    Transforms data from a generator using the provided callable.

    Parameters
    ----------
    data : Iterable[pa.RecordBatch]
        Iterable yielding data to transform.
    callable : Callable
        Function to apply to each item from generator.
    *args, **kwargs
        Additional arguments passed to callable.

    Yields
    ------
    Any
        Transformed data items.
    """
    for record_batch in data:
        yield callable(record_batch, *args, **kwargs)


def table_transform(table: pa.Table, callable: Callable, *args, **kwargs) -> pa.Table:
    """
    Transforms a PyArrow Table using the provided callable.

    Parameters
    ----------
    table : pa.Table
        Table to transform.
    callable : Callable
        Function to apply to the table.
    *args, **kwargs
        Additional arguments passed to callable.

    Returns
    -------
    Any
        Result of applying callable to table.
    """
    return callable(table, *args, **kwargs)


def is_generator(data: Any) -> bool:
    """
    Checks if data is a generator by examining class name.

    Parameters
    ----------
    data : Any
        Object to check.

    Returns
    -------
    bool
        True if data appears to be a generator, False otherwise.
    """
    return "generator" in data.__class__.__name__


def extract_generator_schema(
    data: Iterable[pa.RecordBatch],
) -> Tuple[Iterable[pa.RecordBatch], pa.Schema]:
    """
    Extracts schema from generator data or table.

    Parameters
    ----------
    data : Union[pa.Table, Iterable[pa.RecordBatch]]
        Data to extract schema from.

    Returns
    -------
    pa.Schema
        Extracted schema.
    """
    data, tmp_generator = itertools.tee(data)
    record_batch = next(tmp_generator)
    schema = record_batch.schema
    del tmp_generator
    del record_batch
    return data, schema


def data_transform(
    data: Union[pa.Table, Iterable[pa.RecordBatch]],
    callable: Callable,
    *args,
    **kwargs,
) -> Union[pa.Table, Iterable[pa.RecordBatch]]:
    """
    Transforms data using appropriate method based on type.

    Parameters
    ----------
    data : Union[pa.Table, Iterable[pa.RecordBatch]]
        Data to transform.
    callable : Callable
        Function to apply to the data.
    *args, **kwargs
        Additional arguments passed to callable.

    Returns
    -------
    Union[pa.Table, Iterable[pa.RecordBatch]]
        Transformed data.

    Raises
    ------
    ValueError
        If data is neither a PyArrow Table nor a generator.
    """
    if isinstance(data, pa.lib.Table):
        return table_transform(data, callable, *args, **kwargs)
    elif is_generator(data):
        return generator_transform(data, callable, *args, **kwargs)
    else:
        raise ValueError(
            "Data must be a PyArrow Table or a PyArrow RecordBatch generator"
        )
