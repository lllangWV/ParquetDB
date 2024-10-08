

import logging
import os
import shutil
from functools import partial
from glob import glob
from multiprocessing import Pool
import traceback
from typing import List, Union

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from parquetdb.core.parquet_datasetdb import ParquetDatasetDB
from parquetdb.utils.general_utils import timeit, is_directory_empty
from parquetdb.utils.pyarrow_utils import combine_tables, merge_schemas, align_table,replace_none_with_nulls


# Logger setup
logger = logging.getLogger(__name__)

class ParquetDB:
    def __init__(self, datasets_dir='', n_cores=8):
        """
        Initializes the ParquetDatabase object.

        Args:
            db_path (str): The path to the root directory of the database.
            n_cores (int): The number of CPU cores to be used for parallel processing.
        """
        self.datasets_dir=datasets_dir

        os.makedirs(self.datasets_dir, exist_ok=True)
        
        self.n_cores = n_cores

        self.output_formats=['batch_generator','table','dataset']
        self.reserved_table_names=['tmp']
        

        self.metadata = {}
        logger.info(f"datasets_dir: { self.datasets_dir}")
        logger.info(f"dataset_names: {self.get_datasets()}")
        logger.info(f"reserved_table_names: {self.reserved_table_names}")
        logger.info(f"n_cores: {self.n_cores}")
        logger.info(f"output_formats: {self.output_formats}")

    @ timeit
    def create(self, 
               data:Union[List[dict],dict,pd.DataFrame],  
               dataset_name:str='main',
               batch_size:int=None,
               schema=None,
               metadata=None,
               normalize_dataset:bool=True,
               normalize_kwagrs:dict=dict(max_rows_per_file=10000,
                                        min_rows_per_group=0,
                                        max_rows_per_group=10000)):
        """
        Adds new data to the database.

        Args:
            data (dict or list of dicts): The data to be added to the database. 
                This must contain
            dataset_name (str): The name of the table to add the data to.
            batch_size (int): The batch size. 
                If provided, create will return a generator that yields batches of data.
            max_rows_per_file (int): The maximum number of rows per file.
            min_rows_per_group (int): The minimum number of rows per group.
            max_rows_per_group (int): The maximum number of rows per group.
            schema (pyarrow.Schema): The schema of the incoming table.
            metadata (dict): Metadata to be added to the table.
            **kwargs: Additional keyword arguments to pass to the create function.
        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDatasetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir, n_cores=self.n_cores)
        dataset_db.create(**all_args)

    @timeit
    def read(
        self, dataset_name:str='main',
        ids: List[int] = None,
        columns: List[str] = None,
        include_cols: bool = True,
        filters: List[pc.Expression] = None,
        output_format: str = 'table',
        batch_size: int = None
        ) -> Union[pa.Table, pa.dataset.Scanner]:
        """
        Reads data from the database.

        Args:
            ids (list): A list of IDs to read. If None, reads all data.
            dataset_name (str): The name of the table to read data from.
            columns (list): A list of columns to include in the returned data. By default, all columns are included.
            include_cols (bool): If True, includes the only the fields listed in columns
                If False, includes all fields except the ones listed in columns.
            filters (List): A list of fliters to apply to the data.
            It should operate on a dataframe and return the modifies dataframe
            batch_size (int): The batch size. 
                If provided, read will return a generator that yields batches of data.

        Returns:
            pandas.DataFrame or list: The data read from the database. If deserialize_data is True,
            returns a list of dictionaries with their 'id's. Otherwise, returns the DataFrame with serialized data.
        """

        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDatasetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir, n_cores=self.n_cores)
        return dataset_db.read(**all_args)
    
    @timeit
    def update(self, data: Union[List[dict], dict, pd.DataFrame], dataset_name:str='main', field_type_dict=None):
        """
        Updates data in the database.

        Args:
            data (dict or list of dicts or pandas.DataFrame): The data to be updated.
                Each dict should have an 'id' key corresponding to the record to update.
            dataset_name (str): The name of the table to update data in.
            field_type_dict (dict): A dictionary where the keys are the field names and the values are the new field types.

            **kwargs: Additional keyword arguments.

        Raises:
            ValueError: If new fields are found in the update data that do not exist in the schema.
        """

        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDatasetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir, n_cores=self.n_cores)
        dataset_db.update(**all_args)

    @timeit
    def delete(self, ids:List[int], dataset_name:str='main'):
        """
        Deletes data from the database.

        Args:
            ids (list): A list of IDs to delete.
            dataset_name (str): The name of the table to delete data from.

        Returns:
            None
        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDatasetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir, n_cores=self.n_cores)
        dataset_db.delete(**all_args)

    
    def normalize(self, dataset_name:str='main', schema=None, batch_size: int = None, output_format: str = 'table',
              max_rows_per_file: int = 10000, min_rows_per_group: int = 0, max_rows_per_group: int = 10000,
              existing_data_behavior: str = 'overwrite_or_ignore', **kwargs):
        """
        Normalize the dataset by restructuring files for consistent row distribution.

        This method optimizes performance by ensuring that files in the dataset directory have a consistent number of rows. 
        It first creates temporary files from the current dataset and rewrites them, ensuring that no file has significantly 
        fewer rows than others, which can degrade performance. This is particularly useful after a large data ingestion, 
        as it enhances the efficiency of create, read, update, and delete operations.

        Parameters
        ----------
        schema : Schema, optional
            The schema to use for the dataset. If not provided, it will be inferred from the existing data (default: None).
        batch_size : int, optional
            The number of rows to process in each batch. Required if `output_format` is set to 'batch_generator' (default: None).
        output_format : str, optional
            The format of the output dataset. Supported formats are 'table' and 'batch_generator' (default: 'table').
        max_rows_per_file : int, optional
            The maximum number of rows allowed per file (default: 10,000).
        min_rows_per_group : int, optional
            The minimum number of rows per row group within each file (default: 0).
        max_rows_per_group : int, optional
            The maximum number of rows per row group within each file (default: 10,000).
        existing_data_behavior : str, optional
            Specifies how to handle existing data in the dataset directory. Options are 'overwrite_or_ignore' 
            (default: 'overwrite_or_ignore').
        **kwargs : dict, optional
            Additional keyword arguments passed to the dataset writing process, such as 'max_partitions' or 'max_open_files'.

        Returns
        -------
        None
            This function does not return anything but modifies the dataset directory in place.

        Examples
        --------
        >>> dataset.normalize(
        ...     batch_size=1000,
        ...     output_format='batch_generator',
        ...     max_rows_per_file=5000,
        ...     min_rows_per_group=500,
        ...     max_rows_per_group=5000,
        ...     existing_data_behavior='overwrite_or_ignore',
        ...     max_partitions=512
        ... )
        """
        
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        kwargs=all_args.pop('kwargs')
        all_args.update(kwargs)
        dataset_db=ParquetDatasetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir, n_cores=self.n_cores)
        dataset_db.normalize(**all_args)
    
    @timeit
    def update_schema(self, dataset_name:str='main', field_dict:dict=None, schema:pa.Schema=None):
        """
        Updates the schema of the table.

        Args:
            dataset_name (str): The name of the table to update the schema of.
            field_dict (dict): A dictionary where the keys are the field names and the values are the new field types.
            schema (pyarrow.Schema): The new schema for the table.

        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDatasetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir, n_cores=self.n_cores)
        dataset_db.update_schema(**all_args)

    def get_datasets(self):
        """Get a list of all tables in the database."""
        return os.listdir(self.datasets_dir)
    
    def get_current_files(self, dataset_name:str='main'):
        """Get a list of all files in the dataset directory."""
        dataset_db=ParquetDatasetDB(dataset_name=dataset_name, dir=self.datasets_dir, n_cores=self.n_cores)
        return dataset_db.get_current_files()
    
    def dataset_exists(self, dataset_name:str):
        """Checks if a table exists.

        Args:
            dataset_name (str): The name of the table.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDatasetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir, n_cores=self.n_cores)
        return dataset_db.dataset_exists()
    
    def get_schema(self, dataset_name:str='main'):
        """Get the schema of a table.

        Args:
            dataset_name (str): The name of the table.

        Returns:
            pyarrow.Schema: The schema of the table.
        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDatasetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir, n_cores=self.n_cores)
        return dataset_db.get_schema()
    
    def get_metadata(self, dataset_name:str='main'):
        """Get the metadata of a table.
        
        Args:
            dataset_name (str): The name of the table.

        Returns:
            dict: The metadata of the table.
        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDatasetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir, n_cores=self.n_cores)
        return dataset_db.get_metadata()
    
    def set_metadata(self, dataset_name:str='main', metadata:dict=None):
        """Set the metadata of a table.

        Args:
            dataset_name (str): The name of the table.
            metadata (dict): The metadata to set.
        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDatasetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir, n_cores=self.n_cores)
        dataset_db.set_metadata(**all_args)

    def drop_dataset(self, dataset_name:str='main'):
        """
        Drops a table. by removing the table directory.

        Args:
            dataset_name (str): The name of the table to drop.
        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDatasetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir, n_cores=self.n_cores)
        dataset_db.drop_dataset()
    
    def rename_dataset(self, dataset_name:str='main', new_name:str=None):
        """
        Renames a table.

        Args:
            old_name (str): The current name of the table.
            new_name (str): The new name of the table.
        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDatasetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir, n_cores=self.n_cores)
        dataset_db.rename_dataset(**all_args)

    def copy_dataset(self, dataset_name:str='main', dest_name:str=None, overwrite:bool=False):
        """
        Copies a table to a new table.

        Args:
            dataset_name (str): The name of the source table.
            dest_name (str): The name of the destination table.
        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDatasetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir, n_cores=self.n_cores)
        dataset_db.copy_dataset(**all_args)

    def optimize_dataset(self, 
                         dataset_name:str='main',
                        max_rows_per_file=10000,
                        min_rows_per_group=0,
                        max_rows_per_group=10000,
                        batch_size=None,
                        **kwargs):
        """
        Optimizes the table by merging small Parquet files.

        Args:
            dataset_name (str): The name of the table to optimize.
            max_rows_per_file (int): The maximum number of rows per file.
            min_rows_per_group (int): The minimum number of rows per group.
            max_rows_per_group (int): The maximum number of rows per group.
            batch_size (int): The batch size.
            **kwargs: Additional keyword arguments to pass to the pq.write_to_dataset function.
        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDatasetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir, n_cores=self.n_cores)
        dataset_db.optimize_dataset(**all_args)

    def export_dataset(self, dataset_name:str='main', file_path: str=None, format: str = 'csv'):
        """
        Exports the table to a specified file format.

        Args:
            dataset_name (str): The name of the table to export.
            file_path (str): The file path to export the data to.
            format (str): The format to export ('csv', 'json').
        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDatasetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir, n_cores=self.n_cores)
        dataset_db.export_dataset(**all_args)
        
    def export_partitioned_dataset(self,
                                   export_dir: str,
                                   partitioning,
                                   dataset_name:str='main',
                                   partitioning_flavor=None,
                                   batch_size: int = None, 
                                   **kwargs):
        """
        This method exports a partitioned dataset to a specified file format.

        Args:
            export_dir (str): The directory to export the data to.
            dataset_name (str): The name of the table to export.
            partitioning (dict): The partitioning to use for the dataset.
            partitioning_flavor (str): The partitioning flavor to use.
            batch_size (int): The batch size.
            **kwargs: Additional keyword arguments to pass to the pq.write_to_dataset function.

        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDatasetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir, n_cores=self.n_cores)
        dataset_db.export_partitioned_dataset(**all_args)

    def import_dataset(self, dataset_name:str='main', file_path: str=None, format: str = 'csv', **kwargs):
        """
        Imports a table from a specified file format.

        Args:
            
            dataset_name (str): The name of the table to import the data into.
            file_path (str): The file path to import the data from.
            format (str): The format to import ('csv', 'json').
            **kwargs: Additional keyword arguments to pass to the create function.
        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDatasetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir, n_cores=self.n_cores)
        dataset_db.import_dataset(**all_args)

    def merge_datasets(self, source_tables: List[str], dest_table: str):
        raise NotImplementedError
    
    def backup_database(self, dataset_name:str='main', backup_path: str=None):
        """
        Creates a backup of the database.

        Args:
            backup_path (str): The path where the backup will be stored.
        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDatasetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir, n_cores=self.n_cores)
        dataset_db.backup_database(**all_args)

    def restore_database(self, dataset_name:str='main', backup_path: str=None):
        """
        Restores the database from a backup.

        Args:
            backup_path (str): The path to the backup to restore from.
        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDatasetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir, n_cores=self.n_cores)
        dataset_db.restore_database(**all_args)
