

import logging
import os
import shutil
from glob import glob
from multiprocessing import Pool
import traceback
from typing import List, Union

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

from parquetdb.core.parquetdb import ParquetDB
from parquetdb.utils.general_utils import timeit


# Logger setup
logger=logging.getLogger(__name__)


class ParquetDBManager:
    def __init__(self, datasets_dir=''):
        """
        Initializes the ParquetDBManager object with a specified directory for datasets.

        Parameters
        ----------
        datasets_dir : str, optional
            The path to the root directory where datasets are stored. If not provided, it defaults to the current directory.

        Example
        -------
        >>> manager = ParquetDBManager(datasets_dir='data/parquet_files', n_cores=4)
        datasets_dir: data/parquet_files
        dataset_names: []
        reserved_table_names: ['tmp']
        output_formats: ['batch_generator', 'table', 'dataset']
        """
        self.datasets_dir=datasets_dir

        os.makedirs(self.datasets_dir, exist_ok=True)

        self.output_formats=['batch_generator','table','dataset']
        self.reserved_table_names=['tmp']
        

        self.metadata = {}
        logger.info(f"datasets_dir: { self.datasets_dir}")
        logger.info(f"dataset_names: {self.get_datasets()}")
        logger.info(f"reserved_table_names: {self.reserved_table_names}")
        logger.info(f"output_formats: {self.output_formats}")

    @ timeit
    def create(self, 
               data:Union[List[dict],dict,pd.DataFrame],  
               dataset_name:str='main',
               batch_size:int=None,
               schema=None,
               metadata=None,
               normalize_dataset:bool=False,
               normalize_kwargs:dict=dict(max_rows_per_file=100000,
                                        min_rows_per_group=0,
                                        max_rows_per_group=100000)):
        """
        Adds new data to the database.

        Parameters
        ----------
        data : Union[List[dict], dict, pd.DataFrame]
            The data to be added to the database. It can be in the form of a list of dictionaries,
            a single dictionary, or a pandas DataFrame.
        dataset_name : str, optional
            The name of the dataset or table to add the data to. Default is 'main'.
        batch_size : int, optional
            If provided, the data will be processed in batches of this size.
        schema : pyarrow.Schema, optional
            The schema for the data being added. If not provided, it will be inferred.
        metadata : dict, optional
            Additional metadata to store alongside the data.
        normalize_dataset : bool, optional
            If True, applies normalization to the dataset before saving. Default is True.
        normalize_kwargs : dict, optional
            Keyword arguments to control dataset normalization, such as max rows per file or group.

        Example
        -------
        >>> manager.create(data=[{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}],
        ...                dataset_name='users', batch_size=100)
        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir)
        dataset_db.create(**all_args)

    @timeit
    def read(
        self, dataset_name:str='main',
        ids: List[int] = None,
        columns: List[str] = None,
        include_cols: bool = True,
        filters: List[pc.Expression] = None,
        output_format: str = 'table',
        batch_size: int = None,
        load_kwargs: dict = None
        ):
        """
        Reads data from the database.

        Parameters
        ----------
        dataset_name : str, optional
            The name of the dataset or table to read data from. Default is 'main'.
        ids : list of int, optional
            A list of record IDs to filter the data by. If None, reads all data.
        columns : list of str, optional
            A list of column names to include in the result. If None, all columns are included.
        include_cols : bool, optional
            If True, only the columns in `columns` are returned. If False, all columns except those in `columns` are returned.
        filters : list of pyarrow.compute.Expression, optional
            A list of filters to apply to the data.
        output_format : str, optional
            The format in which to return the data. Options are 'table', 'batch_generator', or 'dataset'. Default is 'table'.
        batch_size : int, optional
            If provided, returns a generator that yields batches of data of this size.
        load_kwargs : dict, optional
            Additional arguments passed to the data loading function (default is None).

        Returns
        -------
        Union[pa.Table, generator, or dataset]
            The data read from the database. If `batch_size` is specified, a generator is returned. Otherwise, it returns a pyarrow Table or Scanner.

        Example
        -------
        >>> data = manager.read(dataset_name='users', columns=['id', 'name'], batch_size=100)
        """

        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir)
        return dataset_db.read(**all_args)
    
    @timeit
    def update(self, data: Union[List[dict], dict, pd.DataFrame], 
               dataset_name:str='main',
               normalize_kwargs:dict=dict(max_rows_per_file=100000,
                                        min_rows_per_group=0,
                                        max_rows_per_group=100000)):
        """
        Updates existing data in the database.

        Parameters
        ----------
        data : Union[List[dict], dict, pd.DataFrame]
            The data to be updated. It can be a list of dictionaries, a single dictionary, or a pandas DataFrame.
            Each entry should include an 'id' field corresponding to the record to update.
        dataset_name : str, optional
            The name of the dataset or table to update data in. Default is 'main'.
        normalize_kwargs : dict, optional
            Additional keyword arguments passed to the normalization process (default is a dictionary with row group settings).

        Example
        -------
        >>> manager.update(data={'id': 1, 'name': 'Alice'}, dataset_name='users')
        """

        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir)
        dataset_db.update(**all_args)

    @timeit
    def delete(self, ids:List[int], 
               dataset_name:str='main',
               normalize_kwargs:dict=dict(max_rows_per_file=100000,
                                        min_rows_per_group=0,
                                        max_rows_per_group=100000)):
        """
        Deletes records from the database.

        Parameters
        ----------
        ids : list of int
            A list of IDs corresponding to the records to be deleted.
        dataset_name : str, optional
            The name of the dataset or table from which to delete records. Default is 'main'.
        normalize_kwargs : dict, optional
            Additional keyword arguments passed to the normalization process (default is a dictionary with row group settings).

        Returns
        -------
        None

        Example
        -------
        >>> manager.delete(ids=[1, 2, 3], dataset_name='users')
        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir)
        dataset_db.delete(**all_args)

    
    def normalize(self, dataset_name:str='main', schema=None, batch_size: int = None, output_format: str = 'table',
              max_rows_per_file: int = 100000, min_rows_per_group: int = 0, max_rows_per_group: int = 100000,
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
        dataset_db=ParquetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir)
        dataset_db.normalize(**all_args)
    
    @timeit
    def update_schema(self, dataset_name:str='main', 
                      field_dict:dict=None, 
                      schema:pa.Schema=None,
                      normalize_kwargs:dict=dict(max_rows_per_file=100000,
                                        min_rows_per_group=0,
                                        max_rows_per_group=100000)):
        """
        Updates the schema of the specified dataset.

        Parameters
        ----------
        dataset_name : str, optional
            The name of the dataset whose schema will be updated. Default is 'main'.
        field_dict : dict, optional
            A dictionary mapping field names to their new data types.
        schema : pyarrow.Schema, optional
            A new schema to apply to the dataset. If provided, it will override the existing schema.
        normalize_kwargs : dict, optional
            Additional keyword arguments passed to the normalization process (default is a dictionary with row group settings).

        Example
        -------
        >>> manager.update_schema(dataset_name='users', field_dict={'age': pa.int32()})
        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir)
        dataset_db.update_schema(**all_args)

    def get_datasets(self):
        """
        Retrieves a list of all datasets (tables) in the database.

        Returns
        -------
        list of str
            A list of dataset (table) names available in the database.

        Example
        -------
        >>> datasets = manager.get_datasets()
        ['users', 'orders', 'transactions']
        """
        return os.listdir(self.datasets_dir)
    
    def get_current_files(self, dataset_name:str='main'):
        """
        Retrieves a list of all current files in the dataset's directory.

        Parameters
        ----------
        dataset_name : str, optional
            The name of the dataset. Default is 'main'.

        Returns
        -------
        list of str
            A list of filenames in the specified dataset's directory.

        Example
        -------
        >>> files = manager.get_current_files(dataset_name='users')
        ['users-1.parquet', 'users-2.parquet']
        """
        dataset_db=ParquetDB(dataset_name=dataset_name, dir=self.datasets_dir)
        return dataset_db.get_current_files()
    
    def dataset_exists(self, dataset_name:str):
        """
        Checks if a dataset (table) exists in the database.

        Parameters
        ----------
        dataset_name : str
            The name of the dataset (table) to check.

        Returns
        -------
        bool
            True if the dataset exists, False otherwise.

        Example
        -------
        >>> exists = manager.dataset_exists('users')
        True
        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir)
        return dataset_db.dataset_exists()
    
    def get_schema(self, dataset_name:str='main'):
        """
        Retrieves the schema of a specified dataset (table).

        Parameters
        ----------
        dataset_name : str, optional
            The name of the dataset (table). Default is 'main'.

        Returns
        -------
        pyarrow.Schema
            The schema of the specified dataset.

        Example
        -------
        >>> schema = manager.get_schema(dataset_name='users')
        Schema for 'users': {'id': 'int64', 'name': 'string', ...}
        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir)
        return dataset_db.get_schema()
    
    def get_metadata(self, dataset_name:str='main'):
        """
        Retrieves the metadata of a specified dataset (table).

        Parameters
        ----------
        dataset_name : str, optional
            The name of the dataset (table). Default is 'main'.

        Returns
        -------
        dict
            A dictionary containing the metadata of the specified dataset.

        Example
        -------
        >>> metadata = manager.get_metadata(dataset_name='users')
        {'created_at': '2023-10-05', 'source': 'csv_import', ...}
        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir)
        return dataset_db.get_metadata()
    
    def set_metadata(self, dataset_name:str='main', metadata:dict=None):
        """
        Sets the metadata for a specified dataset (table).

        Parameters
        ----------
        dataset_name : str, optional
            The name of the dataset (table) to set metadata for. Default is 'main'.
        metadata : dict, optional
            A dictionary containing the metadata to set.

        Example
        -------
        >>> manager.set_metadata(dataset_name='users', metadata={'source': 'api', 'updated_at': '2023-10-05'})
        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir)
        dataset_db.set_metadata(**all_args)

    def drop_dataset(self, dataset_name:str='main'):
        """
        Drops (deletes) a dataset (table) by removing its directory.

        Parameters
        ----------
        dataset_name : str, optional
            The name of the dataset (table) to drop. Default is 'main'.

        Example
        -------
        >>> manager.drop_dataset(dataset_name='users')
        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir)
        dataset_db.drop_dataset()
    
    def rename_dataset(self, dataset_name:str='main', new_name:str=None):
        """
        Renames a dataset (table).

        Parameters
        ----------
        dataset_name : str, optional
            The current name of the dataset (table). Default is 'main'.
        new_name : str
            The new name for the dataset.

        Example
        -------
        >>> manager.rename_dataset(dataset_name='users', new_name='clients')
        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir)
        dataset_db.rename_dataset(**all_args)

    def copy_dataset(self, dataset_name:str='main', dest_name:str=None, overwrite:bool=False):
        """
        Copies a dataset to a new location or table.

        Parameters
        ----------
        dataset_name : str, optional
            The name of the source dataset to copy. Default is 'main'.
        dest_name : str, optional
            The name of the destination dataset. If not provided, the source name is used with a different path.
        overwrite : bool, optional
            If True, overwrites the destination dataset if it already exists. Default is False.

        Example
        -------
        >>> manager.copy_dataset(dataset_name='users', dest_name='users_copy', overwrite=True)
        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir)
        dataset_db.copy_dataset(**all_args)

    def optimize_dataset(self, 
                         dataset_name:str='main',
                        max_rows_per_file=10000,
                        min_rows_per_group=0,
                        max_rows_per_group=10000,
                        batch_size=None,
                        **kwargs):
        """
        Optimizes the dataset by merging smaller Parquet files into larger ones.

        Parameters
        ----------
        dataset_name : str, optional
            The name of the dataset to optimize. Default is 'main'.
        max_rows_per_file : int, optional
            The maximum number of rows per file after optimization. Default is 10,000.
        min_rows_per_group : int, optional
            The minimum number of rows per group. Default is 0.
        max_rows_per_group : int, optional
            The maximum number of rows per group. Default is 10,000.
        batch_size : int, optional
            If provided, the optimization will be performed in batches of this size.
        **kwargs : dict
            Additional keyword arguments passed to the Parquet writing function.

        Example
        -------
        >>> manager.optimize_dataset(dataset_name='users', max_rows_per_file=5000)
        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir)
        dataset_db.optimize_dataset(**all_args)

    def export_dataset(self, dataset_name:str='main', file_path: str=None, format: str = 'csv'):
        """
        Exports a dataset to a specified file format.

        Parameters
        ----------
        dataset_name : str, optional
            The name of the dataset to export. Default is 'main'.
        file_path : str, optional
            The path where the dataset will be exported. If not provided, it will use a default location.
        format : str, optional
            The format to export the dataset as. Supported formats are 'csv' and 'json'. Default is 'csv'.

        Example
        -------
        >>> manager.export_dataset(dataset_name='users', file_path='users_export.csv', format='csv')
        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir)
        dataset_db.export_dataset(**all_args)
        
    def export_partitioned_dataset(self,
                                   export_dir: str,
                                   partitioning,
                                   dataset_name:str='main',
                                   partitioning_flavor=None,
                                   batch_size: int = None, 
                                   **kwargs):
        """
        Exports a partitioned dataset to a specified directory.

        Parameters
        ----------
        export_dir : str
            The directory where the partitioned dataset will be exported.
        dataset_name : str, optional
            The name of the dataset to export. Default is 'main'.
        partitioning : dict
            Defines how the data should be partitioned. The keys are column names, and values are partition criteria.
        partitioning_flavor : str, optional
            The partitioning flavor to use (e.g., 'hive'). Default is None.
        batch_size : int, optional
            If provided, the export will be done in batches of this size.
        **kwargs : dict
            Additional keyword arguments passed to the Parquet writing function.

        Example
        -------
        >>> manager.export_partitioned_dataset(export_dir='partitioned_data', 
                                            partitioning={'country': 'US'},
                                            dataset_name='sales')
        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir)
        dataset_db.export_partitioned_dataset(**all_args)

    def import_dataset(self, dataset_name:str='main', file_path: str=None, format: str = 'csv', **kwargs):
        """
        Imports a dataset from a file into the database.

        Parameters
        ----------
        dataset_name : str, optional
            The name of the dataset to import into. Default is 'main'.
        file_path : str, optional
            The path of the file to import. If not provided, an error is raised.
        format : str, optional
            The format of the file to import. Supported formats are 'csv' and 'json'. Default is 'csv'.
        **kwargs : dict
            Additional keyword arguments passed to the dataset creation function.

        Example
        -------
        >>> manager.import_dataset(dataset_name='users', file_path='users.csv', format='csv')
        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir)
        dataset_db.import_dataset(**all_args)

    def merge_datasets(self, source_tables: List[str], dest_table: str):
        raise NotImplementedError
    
    def backup_database(self, dataset_name:str='main', backup_path: str=None):
        """
        Creates a backup of the specified dataset.

        Parameters
        ----------
        dataset_name : str, optional
            The name of the dataset to back up. Default is 'main'.
        backup_path : str, optional
            The path where the backup will be stored. If not provided, a default backup path is used.

        Example
        -------
        >>> manager.backup_database(dataset_name='users', backup_path='backup/users_backup.parquet')
        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir)
        dataset_db.backup_database(**all_args)

    def restore_database(self, dataset_name:str='main', backup_path: str=None):
        """
        Restores the specified dataset from a backup.

        Parameters
        ----------
        dataset_name : str, optional
            The name of the dataset to restore. Default is 'main'.
        backup_path : str, optional
            The path to the backup file to restore from.

        Example
        -------
        >>> manager.restore_database(dataset_name='users', backup_path='backup/users_backup.parquet')
        """
        all_args = {k: v for k, v in locals().items() if k != 'self'}
        dataset_db=ParquetDB(dataset_name=all_args.pop('dataset_name'), dir=self.datasets_dir)
        dataset_db.restore_database(**all_args)
