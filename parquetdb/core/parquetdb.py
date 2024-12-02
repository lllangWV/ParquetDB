import copy
from dataclasses import dataclass
import logging
import os
import shutil
from glob import glob
import time
import itertools
from typing import Callable, List, Optional, Union

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.fs as fs

from parquetdb import config
from parquetdb.utils.general_utils import timeit, is_directory_empty
from parquetdb.utils import pyarrow_utils

# Logger setup
logger = logging.getLogger(__name__)


@dataclass
class NormalizeConfig:
    """
    Configuration for the normalization process, optimizing performance by managing row distribution and file structure.
    
    Parameters
    ----------
    load_format : str
        The format of the output dataset. Supported formats are 'table' and 'batches' (default: 'table').
    batch_size : int, optional
        The number of rows to process in each batch (default: None).
    batch_readahead : int, optional
        The number of batches to read ahead in a file (default: 16).
    fragment_readahead : int, optional
        The number of files to read ahead, improving IO utilization at the cost of RAM usage (default: 4).
    fragment_scan_options : Optional[pa.dataset.FragmentScanOptions], optional
        Options specific to a particular scan and fragment type, potentially changing across scans.
    use_threads : bool, optional
        Whether to use maximum parallelism determined by available CPU cores (default: True).
    memory_pool : Optional[pa.MemoryPool], optional
        The memory pool for allocations. Defaults to the system's default memory pool.
    filesystem : pyarrow.fs.FileSystem, optional
        Filesystem for writing the dataset (default: None).
    file_options : pyarrow.fs.FileWriteOptions, optional
        Options for writing the dataset files (default: None).
    use_threads : bool
        Whether to use threads for writing (default: True).
    max_partitions : int
        Maximum number of partitions for dataset writing (default: 1024).
    max_open_files : int
        Maximum open files for dataset writing (default: 1024).
    max_rows_per_file : int
        Maximum rows per file (default: 10,000).
    min_rows_per_group : int
        Minimum rows per row group within each file (default: 0).
    max_rows_per_group : int
        Maximum rows per row group within each file (default: 10,000).
    existing_data_behavior : str
        How to handle existing data in the dataset directory (options: 'overwrite_or_ignore', default: 'overwrite_or_ignore').
    create_dir : bool
        Whether to create the dataset directory if it does not exist (default: True).
    """
    load_format: str = 'table'
    batch_size: int = 131_072
    batch_readahead: int = 16
    fragment_readahead: int = 4
    fragment_scan_options: Optional[pa.dataset.FragmentScanOptions] = None
    use_threads: bool = True
    memory_pool: Optional[pa.MemoryPool] = None
    filesystem: Optional[fs.FileSystem] = None
    file_options: Optional[ds.FileWriteOptions] = None
    use_threads: bool = config.parquetdb_config.normalize_kwargs.use_threads
    max_partitions: int = config.parquetdb_config.normalize_kwargs.max_partitions
    max_open_files: int = config.parquetdb_config.normalize_kwargs.max_open_files
    max_rows_per_file: int = config.parquetdb_config.normalize_kwargs.max_rows_per_file
    min_rows_per_group: int = config.parquetdb_config.normalize_kwargs.min_rows_per_group
    max_rows_per_group: int = config.parquetdb_config.normalize_kwargs.max_rows_per_group
    file_visitor: Optional[Callable] = None
    existing_data_behavior: str = config.parquetdb_config.normalize_kwargs.existing_data_behavior
    create_dir: bool = True
    
@dataclass
class LoadConfig:
    """
    Configuration for loading data, specifying columns, filters, batch size, and memory usage.

    Parameters
    ----------
    batch_size : int, optional
        The number of rows to process in each batch (default: 131_072).
    batch_readahead : int, optional
        The number of batches to read ahead in a file (default: 16).
    fragment_readahead : int, optional
        The number of files to read ahead, improving IO utilization at the cost of RAM usage (default: 4).
    fragment_scan_options : Optional[pa.dataset.FragmentScanOptions], optional
        Options specific to a particular scan and fragment type, potentially changing across scans.
    use_threads : bool, optional
        Whether to use maximum parallelism determined by available CPU cores (default: True).
    memory_pool : Optional[pa.MemoryPool], optional
        The memory pool for allocations. Defaults to the system's default memory pool.
    """
    
    batch_size: int = 131_072
    batch_readahead: int = 16
    fragment_readahead: int = 4
    fragment_scan_options: Optional[pa.dataset.FragmentScanOptions] = None
    use_threads: bool = True
    memory_pool: Optional[pa.MemoryPool] = None

class ParquetDB:
    def __init__(self, dataset_name, dir=''):
        """
        Initializes the ParquetDB object.

        Parameters
        ----------
        dataset_name : str
            The name of the dataset to be created or accessed.
        dir : str, optional
            The directory where the dataset will be stored (default is the current directory).
        
        Example
        -------
        >>> db = ParquetDB(dataset_name='my_dataset', dir='/path/to/db', n_cores=4)
        """
        self.dir = dir
        self.dataset_name=dataset_name
        self.dataset_dir=os.path.join(self.dir,self.dataset_name)
        self.basename_template = f'{dataset_name}_{{i}}.parquet'

        os.makedirs(self.dataset_dir, exist_ok=True)

        
        self.load_formats=['batches','table','dataset']

        logger.info(f"dir: {self.dir}")
        logger.info(f"load_formats: {self.load_formats}")

    def create(self, 
               data:Union[List[dict],dict,pd.DataFrame],
               schema:pa.Schema=None,
               metadata:dict=None,
               normalize_dataset:bool=False,
               normalize_config:dict=NormalizeConfig()
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
        normalize_dataset : bool, optional
            If True, the dataset will be normalized after the data is added (default is True).
        normalize_config : NormalizeConfig, optional
            Configuration for the normalization process, optimizing performance by managing row distribution and file structure.
        Example
        -------
        >>> db.create(data=my_data, schema=my_schema, metadata={'source': 'api'}, normalize_dataset=True)
        """
        
        logger.info("Creating data")
        os.makedirs(self.dataset_dir, exist_ok=True)
        
        # Construct incoming table from the data
        incoming_table = self._construct_table(data, schema=schema, metadata=metadata)
        
        
        if 'id' in incoming_table.column_names:
            raise ValueError("When create is called, the data cannot contain an 'id' column.")
        new_ids = self._get_new_ids(incoming_table)
        incoming_table=incoming_table.append_column(pa.field('id', pa.int64()), [new_ids])

        incoming_table = self._preprocess_table(incoming_table)
                
        # If this is the first table, save it directly
        if is_directory_empty(self.dataset_dir):
            incoming_save_path = self._get_save_path()
            pq.write_table(incoming_table, incoming_save_path)
            return None

        try:
            # Merge Schems
            current_schema = self.get_schema()
            incoming_schema=incoming_table.schema
            merged_schema = pa.unify_schemas([current_schema, incoming_schema],promote_options='permissive')
            
            # Algin Incoming Table with Merged Schema
            modified_incoming_table=pyarrow_utils.table_schema_cast(incoming_table, merged_schema)
            are_schemas_equal=current_schema.equals(modified_incoming_table.schema)
            
            if not are_schemas_equal:
                logger.info(f"Schemas not are equal: {are_schemas_equal}. Normalizing the dataset.")
                self._normalize(schema=merged_schema, normalize_config=normalize_config)
            incoming_save_path = self._get_save_path()
            pq.write_table(modified_incoming_table, incoming_save_path)
            
            if normalize_dataset:
                self._normalize(schema=modified_incoming_table.schema, normalize_config=normalize_config)
                
            logger.info("Creating dataset passed")
        except Exception as e:
            logger.exception(f"exception aligning schemas: {e}")
        return None
    
    def read(self,
        ids: List[int] = None,
        columns: List[str] = None,
        filters: List[pc.Expression] = None,
        load_format: str = 'table',
        batch_size:int=None,
        include_cols: bool = True,
        rebuild_nested_struct: bool = False,
        rebuild_nested_from_scratch: bool = False,
        load_config:LoadConfig=LoadConfig(),
        normalize_config:NormalizeConfig=NormalizeConfig()
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
        
        Example
        -------
        >>> data = db.read(ids=[1, 2, 3], columns=['name', 'age'], filters=[pc.field('age') > 18])
        """
        if batch_size:
            load_config.batch_size=batch_size
            
        logger.info("Reading data")
        if columns:
            columns=self.get_field_names(columns=columns, include_cols=include_cols)

        if filters is None:
            filters = []
            
        # Build filter expression
        filter_expression = self._build_filter_expression(ids, filters)

        dataset_dir=None
        if rebuild_nested_struct:
            dataset_dir=self.dataset_dir + '_nested'
            if (not os.path.exists(dataset_dir) or rebuild_nested_from_scratch):
                self.to_nested(normalize_config=normalize_config,rebuild_nested_from_scratch=rebuild_nested_from_scratch)
        data = self._load_data(columns=columns, filter=filter_expression, 
                               load_format=load_format, 
                               dataset_dir=dataset_dir,
                               load_config=load_config)
        logger.info("Reading data passed")
        return data
    
    def update(self, 
               data: Union[List[dict], dict, pd.DataFrame], 
               schema:pa.Schema=None, 
               metadata:dict=None, 
               normalize_config:NormalizeConfig=NormalizeConfig()):
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
        normalize_config : NormalizeConfig, optional
            Configuration for the normalization process, optimizing performance by managing row distribution and file structure.
        
        Example
        -------
        >>> db.update(data=[{'id': 1, 'name': 'John', 'age': 30}, {'id': 2, 'name': 'Jane', 'age': 25}])
        """

        logger.info("Updating data")
        
        # Construct incoming table from the data
        incoming_table = self._construct_table(data, schema=schema, metadata=metadata)
        
        incoming_table = self._preprocess_table(incoming_table)
        incoming_table=pyarrow_utils.table_schema_cast(incoming_table, incoming_table.schema)

        # Non-exisiting id warning step. This is not really necessary but might be nice for user to check
        # self._validate_id(incoming_table['id'].combine_chunks())

        # Apply update normalization
        self._normalize(incoming_table=incoming_table, normalize_config=normalize_config)

        logger.info(f"Updated {self.dataset_name} table.")

    def delete(self, ids:List[int]=None, columns:List[str]=None, 
               normalize_config:NormalizeConfig=NormalizeConfig()):
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

        Example
        -------
        >>> db.delete(ids=[1, 2, 3])
        """
        if ids is not None and columns is not None:
            raise ValueError("Cannot provide both ids and columns to delete.")
        if ids is None and columns is None:
            raise ValueError("Must provide either ids or columns to delete.")

        logger.info("Deleting data from the database")
        

        if ids:
            ids=set(ids)
            # Check if any of the IDs to delete exist in the table. If not, return None
            current_id_table=self._load_data(columns=['id'], load_format='table')
            filtered_id_table = current_id_table.filter( pc.field('id').isin(ids) )
            if filtered_id_table.num_rows==0:
                logger.info(f"No data found to delete.")
                return None
            
        if columns:
            if 'id' in columns:
                raise ValueError("Cannot delete the 'id' column.")
            # Check if any of the columns to delete exist in the table. If not, return None
            schema=self.get_schema()
            incoming_columns=set(columns)
            current_columns=set(schema.names)
            intersection=current_columns.intersection(incoming_columns)
            if len(intersection)==0:
                logger.info(f"No data found to delete.")
                return None
            
        # Apply delete normalization
        self._normalize(ids=ids, columns=columns, normalize_config=normalize_config)
        
        logger.info(f"Deleted data from {self.dataset_name} dataset.")

    def normalize(self, normalize_config:NormalizeConfig=NormalizeConfig()):
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
        >>> db.normalize(normalize_config=normalize_config)
        """
        self._normalize(normalize_config=normalize_config)
    
    def _normalize(self, 
                nested_dataset_dir=None,
                incoming_table=None, 
                schema=None, 
                ids=None,
                columns=None,
                normalize_config:NormalizeConfig=NormalizeConfig()):
        """
        Normalize the dataset by restructuring files for consistent row distribution.

        This method optimizes performance by ensuring that files in the dataset directory have a consistent number of rows. 
        It first creates temporary files from the current dataset and rewrites them, ensuring that no file has significantly 
        fewer rows than others, which can degrade performance. This is particularly useful after a large data ingestion, 
        as it enhances the efficiency of create, read, update, and delete operations.

        Parameters
        ----------
        nested_dataset_dir : str, optional
            The directory where the nested dataset will be saved. If not provided, it will be inferred from the existing data (default: None).
        incoming_table : pa.Table, optional
            The table to use for the update normalization. If not provided, it will be inferred from the existing data (default: None).
        schema : Schema, optional
            The schema to use for the dataset. If not provided, it will be inferred from the existing data (default: None).
        ids : list of int, optional
            A list of IDs to delete from the dataset. If not provided, it will be inferred from the existing data (default: None).
        columns : list of str, optional
            A list of column names to delete from the dataset. If not provided, it will be inferred from the existing data (default: None).
        normalize_config : NormalizeConfig, optional
            Configuration for the normalization process, optimizing performance by managing row distribution and file structure.
        
        Returns
        -------
        None
            This function does not return anything but modifies the dataset directory in place.

        """

        if normalize_config.load_format=='batches':
            logger.debug(f"Writing data in batches")
            schema= self.get_schema() if schema is None else schema
            update_func=generator_update
            delete_func=generator_delete
            schema_cast_func=generator_schema_cast
            rebuild_nested_func=generator_rebuild_nested_struct
            delete_columns_func=generator_delete_columns
        elif normalize_config.load_format=='table':
            update_func=table_update
            delete_func=table_delete
            delete_columns_func=table_delete_columns
            schema_cast_func=table_schema_cast
            rebuild_nested_func=table_rebuild_nested_struct

        try:
            retrieved_data = self._load_data(load_format=normalize_config.load_format, 
                                             load_config=LoadConfig(**dict(batch_size=normalize_config.batch_size,
                                                                           batch_readahead=normalize_config.batch_readahead,
                                                                           fragment_readahead=normalize_config.fragment_readahead,
                                                                           fragment_scan_options=normalize_config.fragment_scan_options,
                                                                           use_threads=normalize_config.use_threads,
                                                                           memory_pool=normalize_config.memory_pool)))
        except pa.lib.ArrowNotImplementedError as e:
            raise ValueError("The incoming data does not match the schema of the existing data.") from e
        # If incoming data is provided this is an update
        if incoming_table:
            logger.info("This normalization is an update. Applying update function, then normalizing.")
            retrieved_data=update_func(retrieved_data, incoming_table)
            
            if schema:
                schema = pa.unify_schemas([schema, incoming_table.schema],promote_options='default')
                schema=pyarrow_utils.sort_schema(schema)
        
        # If ids are provided this is a delete
        elif ids:
            logger.info("This normalization is an id delete. Applying delete function, then normalizing.")
            retrieved_data=delete_func(retrieved_data, ids)
            
        elif columns:
            logger.info("This normalization is a column delete. Applying delete function, then normalizing.")
            retrieved_data=delete_columns_func(retrieved_data, columns)
        
        # If schema is provided this is a schema update
        elif schema:
            logger.info("This normalization is a schema update. Applying schema cast function, then normalizing.")
            retrieved_data=schema_cast_func(retrieved_data, schema)
            
        dataset_dir=self.dataset_dir
        basename_template=f'tmp_{self.dataset_name}_{{i}}.parquet'
        if nested_dataset_dir:
            logger.info("This normalization is a nested rebuild. Applying rebuild function, then normalizing.")
            dataset_dir=nested_dataset_dir
            basename_template=f'{self.dataset_name}_{{i}}.parquet'
            retrieved_data=rebuild_nested_func(retrieved_data)
            if not isinstance(retrieved_data, pa.lib.Table):
                logger.debug("retrieved_data is a record batch")
                retrieved_data, tmp_generator = itertools.tee(retrieved_data)
                record_batch=next(tmp_generator)
                schema=record_batch.schema
                del tmp_generator
                del record_batch
            
        if isinstance(retrieved_data, pa.lib.Table):
            schema=None
            
        logger.debug(f"Schema: {schema}")
        
        try:
            logger.info(f"Writing dataset to {dataset_dir}")
            logger.info(f"Basename template: {basename_template}")
            
            
            ds.write_dataset(retrieved_data, 
                            dataset_dir,
                            basename_template=basename_template, 
                            schema=schema,
                            format="parquet", 
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
            
            # Remove main files to replace with tmp files
            tmp_files=glob(os.path.join(dataset_dir, f'tmp_{self.dataset_name}_*.parquet'))
            
            if len(tmp_files)!=0:
                main_files=glob(os.path.join(dataset_dir, f'{self.dataset_name}_*.parquet'))
                for file_path in main_files:
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                    
        except Exception as e:
            logger.exception(f"exception writing final table to {self.dataset_dir}: {e}")

        tmp_files=glob(os.path.join(dataset_dir, f'tmp_{self.dataset_name}_*.parquet'))
        for file_path in tmp_files:
            file_name=os.path.basename(file_path).replace('tmp_', '')
            new_file_path=os.path.join(dataset_dir, file_name)
            os.rename(file_path, new_file_path)

    def update_schema(self, 
                      field_dict:dict=None, 
                      schema:pa.Schema=None, 
                      normalize_config:NormalizeConfig=NormalizeConfig()):
        """
        Updates the schema of the table in the dataset.

        Parameters
        ----------
        field_dict : dict, optional
            A dictionary where keys are the field names and values are the new field types.
        schema : pyarrow.Schema, optional
            The new schema to apply to the table.
        normalize_config : NormalizeConfig, optional
            Configuration for the normalization process, optimizing performance by managing row distribution and file structure.
            
        Example
        -------
        >>> db.update_schema(field_dict={'age': pa.int32()})
        """
        logger.info("Updating schema")
        current_schema=self.get_schema()
        
        # Update current schema
        updated_schema=pyarrow_utils.update_schema(current_schema, schema, field_dict)
        logger.info(f"updated schema names : {updated_schema.names}")
        
        # Apply Schema normalization
        self._normalize(schema=updated_schema, normalize_config=normalize_config)

        logger.info(f"Updated Fields in {self.dataset_name} table.")

    def get_schema(self):
        """
        Retrieves the schema of the dataset table.
        
        Returns
        -------
        pyarrow.Schema
            The schema of the table.

        Example
        -------
        >>> schema = db.get_schema()
        """
        schema = self._load_data(load_format='dataset').schema
        return schema
    
    def get_field_names(self, columns=None, include_cols=True):
        """
        Retrieves the field names from the dataset schema.

        Parameters
        ----------
        columns : list, optional
            A list of specific column names to include.
        include_cols : bool, optional
            If True, includes only the specified columns. If False, includes all columns 
            except the ones in `columns` (default is True).

        Returns
        -------
        list
            A list of field names.

        Example
        -------
        >>> fields = db.get_field_names(columns=['name', 'age'], include_cols=False)
        """
        if not include_cols:
            schema=self.get_schema()
            all_columns = []
            for filed_schema in schema:
                
                # Only want top column names
                max_defintion_level=filed_schema.max_definition_level
                if max_defintion_level!=1:
                    continue

                all_columns.append(filed_schema.name)

            columns = [col for col in all_columns if col not in columns]
        return columns
    
    def get_metadata(self):
        """
        Retrieves the metadata of the dataset table.

        Returns
        -------
        dict
            The metadata of the table.

        Example
        -------
        >>> metadata = db.get_metadata()
        """
        if not self.dataset_exists():
            raise ValueError(f"Dataset {self.dataset_name} does not exist.")
        schema = self.get_schema()
        logger.debug(f"Metadata:\n\n {schema.metadata}\n\n")
        return schema.metadata
    
    def set_metadata(self, metadata:dict):
        """
        Sets or updates the metadata of the dataset table.

        Parameters
        ----------
        metadata : dict
            A dictionary of metadata to set for the table.

        Example
        -------
        >>> db.set_metadata({'source': 'API', 'version': '1.0'})
        """
        # Update metadata in schema and rewrite Parquet files
        self.update_schema(schema=pa.schema(self.get_schema().fields, metadata=metadata))

    def get_current_files(self):
        """
        Retrieves the list of current Parquet files in the dataset directory.

        Returns
        -------
        list of str
            A list of file paths for the current dataset files.

        Example
        -------
        >>> files = db.get_current_files()
        """
        return glob(os.path.join(self.dataset_dir, f'{self.dataset_name}_*.parquet'))
    
    def dataset_exists(self, dataset_name:str=None):
        """
        Checks if the specified dataset exists.

        Parameters
        ----------
        dataset_name : str, optional
            The name of the dataset to check. If None, checks the current dataset.

        Returns
        -------
        bool
            True if the dataset exists, False otherwise.

        Example
        -------
        >>> db.dataset_exists('my_dataset')
        True
        """
        
        if dataset_name:
            dataset_dir=os.path.join(self.dir,dataset_name)
            return os.path.exists(dataset_dir)
        else:
            return len(self.get_current_files()) != 0

    def drop_dataset(self):
        """
        Removes the current dataset directory, effectively dropping the table.

        Returns
        -------
        None

        Example
        -------
        >>> db.drop_dataset()
        """
        logger.info(f"Dropping dataset {self.dataset_name}")
        if os.path.exists(self.dataset_dir):
            shutil.rmtree(self.dataset_dir)
            logger.info(f"Table {self.dataset_name} has been dropped.")
        else:
            logger.warning(f"Table {self.dataset_name} does not exist.")
        logger.info(f"Dataset {self.dataset_name} dropped")
    
    def rename_dataset(self, new_name:str):
        """
        Renames the current dataset to a new name.

        Parameters
        ----------
        new_name : str
            The new name for the dataset.

        Example
        -------
        >>> db.rename_dataset('new_dataset_name')
        """
        logger.info(f"Renaming dataset to {new_name}")
        if not self.dataset_exists():
            raise ValueError(f"Dataset {self.dataset_name} does not exist.")

        old_dir = os.path.join(self.dataset_dir)
        old_name=self.dataset_name
        

        # Rename all files in the old directory
        old_filepaths = glob(os.path.join(old_dir, f'{old_name}_*.parquet'))
        for old_filepath in old_filepaths:
            filename=os.path.basename(old_filepath)
            file_index=filename.split('.')[0].replace('_','')
            new_filepath = os.path.join(old_dir, f'{new_name}_{file_index}.parquet')
            os.rename(old_filepath, new_filepath)

        # Finally, rename the directory
        new_dir = os.path.join(self.dir, new_name)
        os.rename(old_dir, new_dir)

        self.dataset_name=new_name
        self.dataset_dir=new_dir

        logger.info(f"Table {old_name} has been renamed to {new_name}.")

    def copy_dataset(self, dest_name: str, overwrite: bool = False):
        """
        Copies the current dataset to a new dataset.

        Parameters
        ----------
        dest_name : str
            The name of the destination dataset.
        overwrite : bool, optional
            Whether to overwrite the destination dataset if it already exists (default is False).

        Example
        -------
        >>> db.copy_dataset('new_dataset_copy', overwrite=True)
        """
        logger.info(f"Copying dataset to {dest_name}")
        if overwrite and self.dataset_exists(dest_name):
            shutil.rmtree(os.path.join(self.dir, dest_name))
        elif self.dataset_exists(dest_name):
            raise ValueError(f"Dataset {dest_name} already exists.")
        if dest_name in self.reserved_dataset_names:
            raise ValueError(f"Cannot copy to reserved table name: {dest_name}")
        
        source_dir = self.dataset_dir
        source_name=self.dataset_name
        dest_dir = os.path.join(self.dir, dest_name)
        
        os.makedirs(dest_dir, exist_ok=True)

        # Rename all files in the old directory
        old_filepaths = glob(os.path.join(source_dir, f'{source_name}_*.parquet'))
        for old_filepath in old_filepaths:
            filename=os.path.basename(old_filepath)
            file_index=filename.split('.')[0].replace('_','')
            new_filepath = os.path.join(dest_dir, f'{dest_name}_{file_index}.parquet')
            shutil.copyfile(old_filepath, new_filepath)

        logger.info(f"Table {source_name} has been copied to {dest_name}.")

    def export_dataset(self, file_path: str, format: str = 'csv'):
        """
        Exports the dataset to a specified file format.

        Parameters
        ----------
        file_path : str
            The path where the exported file will be saved.
        format : str, optional
            The format for exporting the data ('csv', 'json'). Default is 'csv'.

        Raises
        ------
        ValueError
            If an unsupported export format is provided.

        Example
        -------
        >>> db.export_dataset(file_path='/path/to/file.csv', format='csv')
        """
        table = self._load_data(load_format='table')
        if format == 'csv':
            df = table.to_pandas()
            df.to_csv(file_path, index=False)
        elif format == 'json':
            df = table.to_pandas()
            df.to_json(file_path, orient='records', lines=True)
        else:
            raise ValueError(f"Unsupported export format: {format}")
        logger.info(f"Exported table {self.dataset_name} to {file_path} as {format}.")

    def export_partitioned_dataset(self,
                                   export_dir: str, 
                                   partitioning,
                                   partitioning_flavor=None,
                                   load_config: LoadConfig = LoadConfig(),
                                   load_format: str = 'table',
                                   **kwargs):
        """
        Exports the dataset to a specified directory with partitioning.

        Parameters
        ----------
        export_dir : str
            The directory where the partitioned dataset will be saved.
        partitioning : dict
            The partitioning strategy to use (e.g., by columns).
        partitioning_flavor : str, optional
            The partitioning flavor to use (e.g., 'hive' partitioning).
        **kwargs : dict, optional
            Additional arguments passed to the `pq.write_to_dataset` function.

        Example
        -------
        >>> db.export_partitioned_dataset(export_dir='/path/to/export', partitioning={'year': '2023'}, partitioning_flavor='hive')
        """
        self._validate_load_format(load_format)
        
        logger.info(f"Exporting partitioned dataset to {export_dir}")
        # Read the entire dataset either in batches or as a whole
        retrieved_data = self._load_data(load_format=load_format, load_config=load_config)
        schema=self.get_schema()

        # Can't provide schema to wrrite_to_dataset if the data is a table
        if isinstance(retrieved_data, pa.lib.Table):
            schema=None

        pq.write_to_dataset(
                retrieved_data,
                export_dir,
                schema=schema,
                partitioning=partitioning,
                partitioning_flavor=partitioning_flavor,
                format="parquet",
                **kwargs
            )
        logger.info(f"Partitioned dataset exported to {export_dir}")

    def import_dataset(self, file_path: str, format: str = 'csv', **kwargs):
        """
        Imports data from a specified file into the dataset.

        Parameters
        ----------
        file_path : str
            The path of the file to import.
        format : str, optional
            The format of the file to import ('csv', 'json'). Default is 'csv'.
        **kwargs : dict, optional
            Additional arguments passed to the data loading function (e.g., pandas read options).

        Raises
        ------
        ValueError
            If an unsupported import format is provided.

        Example
        -------
        >>> db.import_dataset(file_path='/path/to/file.csv', format='csv')
        """
        logger.info("Importing data")
        if format == 'csv':
            logger.info("Importing csv")
            df = pd.read_csv(file_path, **kwargs)
        elif format == 'json':
            logger.info("Importing json")
            df = pd.read_json(file_path, **kwargs)
        else:
            raise ValueError(f"Unsupported import format: {format}")
        self.create(data=df)
        logger.info(f"Imported data from {file_path} into table {self.dataset_name}.")

    def merge_datasets(self, source_tables: List[str], dest_table: str):
        raise NotImplementedError
    
    def backup_database(self, backup_path: str):
        """
        Creates a backup of the current dataset by copying it to the specified backup path.

        Parameters
        ----------
        backup_path : str
            The path where the backup will be stored.

        Example
        -------
        >>> db.backup_database(backup_path='/path/to/backup')
        """
        logger.info("Backing up database to : {backup_path}")
        shutil.copytree(self.dataset_dir, backup_path)
        logger.info(f"Database backed up to {backup_path}.")

    def restore_database(self, backup_path: str):
        """
        Restores the dataset from a specified backup path.

        Parameters
        ----------
        backup_path : str
            The path to the backup from which the database will be restored.

        Example
        -------
        >>> db.restore_database(backup_path='/path/to/backup')
        """
        logger.info("Restoring database from : {backup_path}")
        if os.path.exists(self.dataset_dir):
            shutil.rmtree(self.dataset_dir)
        shutil.copytree(backup_path, self.dataset_dir)
        logger.info(f"Database restored from {backup_path}.")

    def to_nested(self, normalize_config:NormalizeConfig=NormalizeConfig(), 
                  rebuild_nested_from_scratch: bool = False):
        """
        Converts the current dataset to a nested dataset.

        Parameters
        ----------
        normalize_config : NormalizeConfig, optional
            Configuration for the normalization process, optimizing performance by managing row distribution and file structure.
        rebuild_nested_from_scratch : bool, optional
            If True, rebuilds the nested structure from scratch (default is False).

        Returns
        -------
        None
            This function does not return anything but modifies the dataset directory in place.

        Examples
        --------
        >>> db.to_nested()
        """

        dataset_name=self.dataset_name
        nested_dataset_name=f'{dataset_name}_nested'
        nested_dataset_dir=os.path.join(self.dir,nested_dataset_name)
        
        if os.path.exists(nested_dataset_dir) and rebuild_nested_from_scratch:
            shutil.rmtree(nested_dataset_dir)
        os.makedirs(nested_dataset_dir, exist_ok=True)
        
        self._normalize(nested_dataset_dir=nested_dataset_dir,  normalize_config=normalize_config)

    def _load_data(self, 
                   load_format:str='table',
                   columns:List[str]=None, 
                   filter:List[pc.Expression]=None, 
                   dataset_dir:str=None,
                   load_config:LoadConfig=LoadConfig()):
        """
        Loads data from the dataset, supporting various output formats such as PyArrow Table, Dataset, or a batch generator.

        Parameters
        ----------
        columns : list of str, optional
            A list of column names to load. If None, all columns are loaded (default is None).
        filter : list of pyarrow.compute.Expression, optional
            A list of filters to apply to the data (default is None).
        load_format : str, optional
            The format for loading the data: 'table', 'batches', or 'dataset' (default is 'table').
        dataset_dir : str, optional
            The directory where the dataset is stored (default is None).
        load_config : LoadConfig, optional
            Configuration for loading data, optimizing performance by managing memory usage.

        Returns
        -------
        Union[pa.Table, pa.dataset.Scanner, Iterator[pa.RecordBatch]]
            The loaded data as a PyArrow Table, Dataset, or batch generator, depending on the specified output format.

        Example
        -------
        >>> data = db._load_data(columns=['name', 'age'], load_format='table')
        """

        if dataset_dir is None:
            dataset_dir=self.dataset_dir
        
        logger.info(f"Loading data from {dataset_dir}")
        logger.info(f"Loading only columns: {columns}")
        logger.info(f"Using filter: {filter}")

        dataset = ds.dataset(dataset_dir, format="parquet")
        if load_format=='batches':
            return self._load_batches(dataset, columns, filter, load_config=load_config)
        elif load_format=='table':
            return self._load_table(dataset, columns, filter, load_config=load_config)
        elif load_format=='dataset':
            logger.info(f"Loading data as an {dataset.__class__} object")
            return dataset
        else:
            raise ValueError(f"load_format must be one of the following: {self.load_formats}")
    
    def _load_batches(self, dataset, 
                      columns:List[str]=None, 
                      filter:List[pc.Expression]=None, 
                      load_config:LoadConfig=LoadConfig()):
        """
        Loads data in batches from the dataset, returning an iterator of PyArrow RecordBatches.

        Parameters
        ----------
        dataset : pa.dataset.Dataset
            The PyArrow dataset from which to load data.
        columns : list of str, optional
            A list of column names to load. If None, all columns are loaded (default is None).
        filter : list of pyarrow.compute.Expression, optional
            A list of filters to apply to the data (default is None).
        load_config : LoadConfig, optional
            Configuration for loading data, optimizing performance by managing memory usage.

        Returns
        -------
        Iterator[pa.RecordBatch]
            An iterator yielding batches of data as PyArrow RecordBatches.

        Example
        -------
        >>> batches = db._load_batches(dataset, columns=['name', 'age'])
        """
 
        try:
            generator=dataset.to_batches(columns=columns, filter=filter, **load_config.__dict__)
            logger.info(f"Loading as a {generator.__class__} object")
        except Exception as e:
            logger.debug(f"Error loading table: {e}. Returning empty table")
            generator=pyarrow_utils.create_empty_batch_generator(schema=dataset.schema, columns=columns)
        return generator
    
    def _load_table(self, dataset, 
                    columns:List[str]=None, 
                    filter:List[pc.Expression]=None, load_config:LoadConfig=LoadConfig()):
        """
        Loads the entire dataset as a PyArrow Table.

        Parameters
        ----------
        dataset : pa.dataset.Dataset
            The PyArrow dataset from which to load data.
        columns : list of str, optional
            A list of column names to load. If None, all columns are loaded (default is None).
        filter : list of pyarrow.compute.Expression, optional
            A list of filters to apply to the data (default is None).
        load_config : LoadConfig, optional
            Configuration for loading data, optimizing performance by managing memory usage.

        Returns
        -------
        pa.Table
            The loaded data as a PyArrow Table.

        Example
        -------
        >>> table = db._load_table(dataset, columns=['name', 'age'])
        """
        try:
            table=dataset.to_table(columns=columns, filter=filter, **load_config.__dict__)
            logger.info(f"Loading data as a {table.__class__} object")
        except Exception as e:
            logger.debug(f"Error loading table: {e}. Returning empty table")
            table=pyarrow_utils.create_empty_table(schema=dataset.schema, columns=columns)
        return table
    
    def _preprocess_table(self, table):
        table=pyarrow_utils.flatten_table(table)
        
        for column_name in table.column_names:
            # Convert list column to fixed tensor
            table=pyarrow_utils.convert_list_column_to_fixed_tensor(table, column_name)
            
            # Replace empty structs with dummy structs
            table=pyarrow_utils.replace_empty_structs_in_column(table, column_name)
            table=pyarrow_utils.flatten_table_in_column(table, column_name)
        return table
    
    def _get_new_ids(self, incoming_table):
        """
        Generates a list of new IDs for the incoming data, starting from the next available ID.

        Parameters
        ----------
        data_list : list of dict
            The incoming data for which new IDs will be generated. Each entry represents a row in the dataset.

        Returns
        -------
        list of int
            A list of new unique IDs for each entry in the data list.

        Example
        -------
        >>> new_ids = db._get_new_ids(data_list=[{'name': 'Alice'}, {'name': 'Bob'}])
        """
        logger.info("Getting new ids")
        if is_directory_empty(self.dataset_dir):
            logger.debug("Directory is empty. Starting id from 0")
            start_id = 0
        else:
            table = self._load_data(columns=['id'],load_format='table')
            max_val=pc.max(table.column('id')).as_py()
            start_id = max_val + 1  # Start from the next available ID
            
            logger.debug(f"Directory is not empty. Starting id from {start_id}")
    
        # Create a list of new IDs
        new_ids = list(range(start_id, start_id + incoming_table.num_rows))
        logger.info("New ids generated")
        return new_ids
    
    def _build_filter_expression(self, ids: List[int], filters: List[pc.Expression]):
        """
        Builds a filter expression from provided IDs and additional filters.

        Parameters
        ----------
        ids : list of int, optional
            A list of IDs to include in the filter.
        filters : list of pyarrow.compute.Expression, optional
            Additional filter expressions to apply to the dataset.

        Returns
        -------
        pyarrow.compute.Expression or None
            A combined filter expression, or None if no filters are provided.

        Example
        -------
        >>> filter_expr = db._build_filter_expression(ids=[1, 2, 3], filters=[pc.field('age') > 18])
        """
        logger.info("Building filter expression")
        final_filters = []
        
        # Add ID filter if provided
        if ids:
            id_filter = pc.field('id').isin(ids)
            final_filters.append(id_filter)

        # Append custom filters
        final_filters.extend(filters)

        # Combine filters into a single filter expression
        if not final_filters:
            return None
        
        filter_expression = final_filters[0]
        for filter_expr in final_filters[1:]:
            filter_expression = filter_expression & filter_expr
        logger.info("Filter expression built")
        return filter_expression

    def _get_save_path(self):
        """
        Determines the path to save the incoming table based on the number of existing files in the dataset directory.

        Returns
        -------
        str
            The file path where the new table will be saved.

        Example
        -------
        >>> save_path = db._get_save_path()
        """
        logger.info("Getting save path")
        n_files = len(glob(os.path.join(self.dataset_dir, f'{self.dataset_name}_*.parquet')))
        save_path=None
        if n_files == 0:
            save_path=os.path.join(self.dataset_dir, f'{self.dataset_name}_0.parquet')
        else:
            save_path=os.path.join(self.dataset_dir, f'{self.dataset_name}_{n_files}.parquet')
        logger.info(f"Save path: {save_path}")
        return save_path

    def _validate_id(self, id_column):
        """
        Validates the incoming ID column by checking if the IDs exist in the main table.

        Parameters
        ----------
        id_column : pyarrow.Array
            The ID column from the incoming table to validate.

        Returns
        -------
        None

        Example
        -------
        >>> db._validate_id(id_column=incoming_table['id'])
        """
        logger.info(f"Validating ids")
        current_table=self.read(columns=['id'], load_format='table').combine_chunks()
        filtered_table = current_table.filter(~pc.field('id').isin(id_column))

        if filtered_table.num_rows==0:
            logger.warning(f"The following ids are not in the main table", extra={'ids_do_not_exist': filtered_table['id'].combine_chunks()})
        return None

    def _validate_load_format(self, load_format):
        if load_format not in self.load_formats:
            raise ValueError(f"load_format must be one of the following: {self.load_formats}")
    
    def _construct_table(self, data, schema=None, metadata=None):
            logger.info("Validating data")
            if isinstance(data, dict):
                logger.info("The incoming data is a dictonary of arrays")
                for key, value in data.items():
                    if not isinstance(value, List):
                        data[key]=[value]
                table=pa.Table.from_pydict(data)
                incoming_array=table.to_struct_array()
                incoming_array=incoming_array.flatten()
                incoming_schema=table.schema
                
            elif isinstance(data, list):
                logger.info("Incoming data is a list of dictionaries")
                # Convert to pyarrow array to get the schema. This method is faster than .from_pylist
                # As from_pylist iterates through record in a python loop, but pa.array handles this in C++/cython
                incoming_array=pa.array(data)
                incoming_schema=pa.schema(incoming_array.type)
                incoming_array=incoming_array.flatten()
                
            elif isinstance(data, pd.DataFrame):
                logger.info("Incoming data is a pandas dataframe")
                table=pa.Table.from_pandas(data)
                incoming_array=table.to_struct_array()
                incoming_array=incoming_array.flatten()
                incoming_schema=table.schema
                
            elif isinstance(data, pa.lib.Table):
                incoming_schema=data.schema
                incoming_array=data.to_struct_array()
                incoming_array=incoming_array.flatten()
                
            else:
                raise TypeError("Data must be a dictionary or a list of dictionaries.")
            
            if schema is None:
                schema=incoming_schema
            schema=schema.with_metadata(metadata)

            incoming_table=pa.Table.from_arrays(incoming_array,schema=schema)
            return incoming_table

def generator_schema_cast(generator, new_schema):
    for record_batch in generator:
        updated_record_batch=pyarrow_utils.table_schema_cast(record_batch, new_schema)
        yield updated_record_batch
        
def table_schema_cast(current_table, new_schema):
    updated_table=pyarrow_utils.table_schema_cast(current_table, new_schema)
    return updated_table
        
        
def table_update(current_table, incoming_table):
    # Merging Schema
    incoming_schema=incoming_table.schema
    current_schema=current_table.schema
    merged_schema = pa.unify_schemas([current_schema, incoming_schema],promote_options='default')
    
    # Aligning current and incoming tables with merged schema
    incoming_table=pyarrow_utils.table_schema_cast(incoming_table, merged_schema)
    current_table = pyarrow_utils.table_schema_cast(current_table, merged_schema)
    
    updated_record_batch=pyarrow_utils.update_flattend_table(current_table, incoming_table)
    return updated_record_batch
        
def generator_update(generator, incoming_table):
    
    incoming_schema=incoming_table.schema
    merged_schema=None
    for record_batch in generator:
        if merged_schema is None:
            current_schema=record_batch.schema
            merged_schema = pa.unify_schemas([current_schema, incoming_schema],promote_options='default')
            incoming_table=pyarrow_utils.table_schema_cast(incoming_table, merged_schema)
            
        record_batch = pyarrow_utils.table_schema_cast(record_batch, merged_schema)
        updated_record_batch=pyarrow_utils.update_flattend_table(record_batch, incoming_table)
        yield updated_record_batch
        
def table_delete(current_table, ids):
    updated_table = current_table.filter( ~pc.field('id').isin(ids) )
    return updated_table

def generator_delete(generator, ids):
    for record_batch in generator:
        updated_record_batch = record_batch.filter( ~pc.field('id').isin(ids) )
        yield updated_record_batch
        
        
def table_delete_columns(current_table, columns):
    updated_table = current_table.drop_columns(columns)
    return updated_table

def generator_delete_columns(generator, columns):
    for record_batch in generator:
        updated_record_batch = record_batch.drop_columns(columns)
        yield updated_record_batch
        
        
        
def table_rebuild_nested_struct(current_table):
    return pyarrow_utils.rebuild_nested_table(current_table)

def generator_rebuild_nested_struct(generator):
    for record_batch in generator:
        updated_record_batch=pyarrow_utils.rebuild_nested_table(record_batch,load_format='batches')
        yield updated_record_batch
        
        
