import logging
import os
import shutil
from glob import glob
from typing import List, Union

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from parquetdb.utils.general_utils import timeit, is_directory_empty
from parquetdb.utils import pyarrow_utils

# Logger setup
logger = logging.getLogger(__name__)

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
        n_cores : int, optional
            The number of CPU cores to use for parallel processing (default is 8).
        
        Example
        -------
        >>> db = ParquetDB(dataset_name='my_dataset', dir='/path/to/db', n_cores=4)
        """
        self.dir = dir
        self.dataset_name=dataset_name
        self.dataset_dir=os.path.join(self.dir,self.dataset_name)
        self.tmp_dir=os.path.join(self.dir,'tmp')
        self.basename_template = f'{dataset_name}_{{i}}.parquet'


        os.makedirs(self.tmp_dir, exist_ok=True)
        os.makedirs(self.dataset_dir, exist_ok=True)

        
        self.output_formats=['batch_generator','table','dataset']
        self.reserved_dataset_names=['tmp']

        logger.info(f"dir: {self.dir}")
        logger.info(f"reserved_dataset_names: {self.reserved_dataset_names}")
        logger.info(f"output_formats: {self.output_formats}")

    @timeit
    def create(self, data:Union[List[dict],dict,pd.DataFrame],
               batch_size:int=None,
               schema=None,
               metadata=None,
               normalize_dataset:bool=False,
               normalize_kwargs:dict=dict(max_rows_per_file=100000,
                                        min_rows_per_group=0,
                                        max_rows_per_group=100000)
               ):
        """
        Adds new data to the database.

        Parameters
        ----------
        data : dict, list of dict, or pandas.DataFrame
            The data to be added to the database.
        batch_size : int, optional
            The batch size. If provided, returns a generator that yields batches of data.
        schema : pyarrow.Schema, optional
            The schema for the incoming data.
        metadata : dict, optional
            Metadata to be attached to the table.
        normalize_dataset : bool, optional
            If True, the dataset will be normalized after the data is added (default is True).
        normalize_kwargs : dict, optional
            Additional arguments for the normalization process (default is a dictionary with row group settings).
        
        Example
        -------
        >>> db.create(data=my_data, batch_size=1000, schema=my_schema, metadata={'source': 'api'}, normalize_dataset=True)
        """
        logger.info("Creating data")
        os.makedirs(self.dataset_dir, exist_ok=True)
        
        # Prepare the data and field data
        data_list, incoming_schema=self._validate_data(data)
        new_ids = self._get_new_ids(data_list)

        if schema is None:
            schema=incoming_schema
        schema=schema.with_metadata(metadata)
        
        # Create the incoming table
        incoming_table=pa.Table.from_pylist(data_list, schema=schema)
        incoming_table=incoming_table.append_column(pa.field('id', pa.int64()), [new_ids])
        
        # Sometimes records have a nested dictionaries and some do not. 
        # This ensures all records have the same nested structs
        incoming_table=pyarrow_utils.replace_empty_structs_in_table(incoming_table)
        
        # We store the flatten table because it is easier to process
        incoming_table=pyarrow_utils.flatten_table(incoming_table)

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
                self.normalize(schema=merged_schema, 
                               batch_size=batch_size, 
                               **normalize_kwargs)
            incoming_save_path = self._get_save_path()
            pq.write_table(modified_incoming_table, incoming_save_path)
            
            if normalize_dataset:
                self.normalize(schema=modified_incoming_table.schema, batch_size=batch_size, **normalize_kwargs)
            logger.info("Creating dataset passed")
        except Exception as e:
            logger.exception(f"exception aligning schemas: {e}")
            logger.info("Restoring original files")
            self._restore_tmp_files()
        return None
    
    @timeit
    def read(
        self,
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
        ids : list of int, optional
            A list of IDs to read. If None, all data is read (default is None).
        columns : list of str, optional
            The columns to include in the output. If None, all columns are included (default is None).
        include_cols : bool, optional
            If True, includes only the specified columns. If False, excludes the specified columns (default is True).
        filters : list of pyarrow.compute.Expression, optional
            Filters to apply to the data (default is None).
        output_format : str, optional
            The format of the returned data: 'table' or 'batch_generator' (default is 'table').
        batch_size : int, optional
            The batch size. If provided, returns a generator yielding batches of data (default is None).
        load_kwargs : dict, optional
            Additional arguments passed to the data loading function (default is None).
        
        Returns
        -------
        pa.Table, generator, or dataset
            The data read from the database. The output can be in table format or as a batch generator.
        
        Example
        -------
        >>> data = db.read(ids=[1, 2, 3], columns=['name', 'age'], filters=[pc.field('age') > 18])
        """
        logger.info("Reading data")
        if columns:
            columns=self.get_field_names(columns=columns, include_cols=include_cols)

        if filters is None:
            filters = []
            
        if batch_size:
            logger.info(f"Found batch_size: {batch_size}. Setting output_format to batch_generator")
            output_format='batch_generator'
            
        # Build filter expression
        filter_expression = self._build_filter_expression(ids, filters)

        data = self._load_data(columns=columns, filter=filter_expression, 
                               batch_size=batch_size, output_format=output_format, load_kwargs=load_kwargs)
        logger.info("Reading data passed")
        return data
    
    @timeit
    def update(self, data: Union[List[dict], dict, pd.DataFrame], 
                    normalize_kwargs=dict(max_rows_per_file=100000,
                                                min_rows_per_group=0,
                                                max_rows_per_group=100000)):
        """
        Updates existing records in the database.

        Parameters
        ----------
        data : dict, list of dicts, or pandas.DataFrame
            The data to be updated in the database. Each record must contain an 'id' key 
            corresponding to the record to be updated.

        Example
        -------
        >>> db.update(data=[{'id': 1, 'name': 'John', 'age': 30}, {'id': 2, 'name': 'Jane', 'age': 25}])
        """
        logger.info("Updating data")
        # Data validation.
        data_list, incoming_schema = self._validate_data(data)
        incoming_table = pa.Table.from_pylist(data_list, schema=incoming_schema)
        
        # Incoming table processing
        incoming_table=pyarrow_utils.replace_empty_structs_in_table(incoming_table)
        incoming_table=pyarrow_utils.flatten_table(incoming_table)
        incoming_table=pyarrow_utils.table_schema_cast(incoming_table, incoming_table.schema)
        
        # Merging Schema
        incoming_schema=incoming_table.schema
        current_schema=self.get_schema()
        merged_schema = pa.unify_schemas([current_schema, incoming_schema],promote_options='default')
        
        # Aligning incoming table with merged schema
        incoming_table=pyarrow_utils.table_schema_cast(incoming_table, merged_schema)
        
        # Non-exisiting id warning step. THis is not really necessary but might be nice for user to check
        self._validate_id(incoming_table['id'].combine_chunks())
        
        # Apply update normalization
        self.normalize(incoming_table=incoming_table, **normalize_kwargs)

        logger.info(f"Updated {self.dataset_name} table.")

    @timeit
    def delete(self, ids:List[int], 
               normalize_kwargs=dict(
                                max_rows_per_file=100000,
                                min_rows_per_group=0,
                                max_rows_per_group=100000)):
        """
        Deletes records from the database.

        Parameters
        ----------
        ids : list of int
            A list of record IDs to delete from the database.
        normalize_kwargs : dict, optional
            Additional keyword arguments passed to the normalization process (default is a dictionary with row group settings).

        Returns
        -------
        None

        Example
        -------
        >>> db.delete(ids=[1, 2, 3])
        """
        logger.info("Deleting data from the database")
        ids=set(ids)

        # Check if any of the IDs to delete exist in the table. If not, return None
        current_id_table=self._load_data(columns=['id'], output_format='table')
        filtered_id_table = current_id_table.filter( pc.field('id').isin(ids) )
        if filtered_id_table.num_rows==0:
            logger.info(f"No data found to delete.")
            return None
        
        # Apply delete normalization
        self.normalize(ids=ids, **normalize_kwargs)
        
        logger.info(f"Deleted data from {self.dataset_name} dataset.")

    @timeit
    def normalize(self, incoming_table=None, schema=None, ids=None, batch_size: int = None, load_kwargs: dict = None, output_format: str = 'table',
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
        incoming_table : pa.Table, optional
            The table to use for the update normalization. If not provided, it will be inferred from the existing data (default: None).
        schema : Schema, optional
            The schema to use for the dataset. If not provided, it will be inferred from the existing data (default: None).
        batch_size : int, optional
            The number of rows to process in each batch. Required if `output_format` is set to 'batch_generator' (default: None).
        load_kwargs : dict, optional
            These are the kwyword arguments to pass to either `dataset.to_batches` or `dataset.to_table`.
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
        if output_format=='batch_generator' and batch_size is None:
            raise ValueError("batch_size must be provided when output_format is batch_generator")
        
        # Create tmp files. 
        # This is done because write dataset will overwrite everything in the main dataset dir
        self._write_tmp_files()
        
        if batch_size:
            logger.debug(f"Writing data in batches")
            output_format='batch_generator'
            schema= self.get_schema(load_tmp=True) if schema is None else schema
            update_func=generator_update
            delete_func=generator_delete
            schema_cast_func=generator_schema_cast
        elif output_format=='table':
            update_func=table_update
            delete_func=table_delete
            schema_cast_func=table_schema_cast

        try:
            retrieved_data = self._load_data(batch_size=batch_size, 
                                             output_format=output_format, 
                                             load_tmp=True, 
                                             load_kwargs=load_kwargs)        
        except pa.lib.ArrowNotImplementedError as e:
            raise ValueError("The incoming data does not match the schema of the existing data.") from e

        # If incoming data is provided this is an update
        if incoming_table:
            logger.info("This normalization is an update. Applying update function, then normalizing.")
            retrieved_data=update_func(retrieved_data, incoming_table)
            schema=incoming_table.schema
        
        # If ids are provided this is a delete
        if ids:
            logger.info("This normalization is a delete. Applying delete function, then normalizing.")
            retrieved_data=delete_func(retrieved_data, ids)
        
        # If schema is provided this is a schema update
        if schema:
            logger.info("This normalization is a schema update. Applying schema cast function, then normalizing.")
            retrieved_data=schema_cast_func(retrieved_data, schema)
            
        if isinstance(retrieved_data, pa.lib.Table):
            schema=None

        try:
            logger.info(f"Writing dataset to {self.dataset_dir}")
            
            ds.write_dataset(retrieved_data, 
                            self.dataset_dir,
                            basename_template=self.basename_template, 
                            schema=schema,
                            format="parquet", 
                            max_partitions=kwargs.get('max_partitions', 1024),
                            max_open_files=kwargs.get('max_open_files', 1024), 
                            max_rows_per_file=max_rows_per_file, 
                            min_rows_per_group=min_rows_per_group, 
                            max_rows_per_group=max_rows_per_group,
                            existing_data_behavior=existing_data_behavior, 
                            **kwargs)
            
        except Exception as e:
            logger.exception(f"exception writing final table to {self.dataset_dir}: {e}")
            logger.info("Restoring original files")
            self._restore_tmp_files()

    @timeit
    def update_schema(self, field_dict:dict=None, schema:pa.Schema=None, 
                      normalize_kwargs=dict(
                                max_rows_per_file=100000,
                                min_rows_per_group=0,
                                max_rows_per_group=100000)):
                                            
        """
        Updates the schema of the table in the dataset.

        Parameters
        ----------
        field_dict : dict, optional
            A dictionary where keys are the field names and values are the new field types.
        schema : pyarrow.Schema, optional
            The new schema to apply to the table.
        normalize_kwargs : dict, optional
            Additional keyword arguments passed to the normalization process (default is a dictionary with row group settings).

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
        self.normalize(schema=updated_schema, **normalize_kwargs)

        logger.info(f"Updated Fields in {self.dataset_name} table.")

    def get_schema(self, load_tmp=False):
        """
        Retrieves the schema of the dataset table.

        Parameters
        ----------
        load_tmp : bool, optional
            Whether to load from temporary files if available (default is False).

        Returns
        -------
        pyarrow.Schema
            The schema of the table.

        Example
        -------
        >>> schema = db.get_schema()
        """
        schema = self._load_data(output_format='dataset', load_tmp=load_tmp).schema
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
        if new_name in self.reserved_dataset_names:
            raise ValueError(f"Cannot rename to reserved table name: {new_name}")
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

    def optimize_dataset(self, 
                    max_rows_per_file=10000,
                    min_rows_per_group=0,
                    max_rows_per_group=10000,
                    batch_size=None,
                    **kwargs):
        """
        Optimizes the dataset by merging small Parquet files into larger files.

        Parameters
        ----------
        max_rows_per_file : int, optional
            The maximum number of rows per Parquet file (default is 10,000).
        min_rows_per_group : int, optional
            The minimum number of rows per group (default is 0).
        max_rows_per_group : int, optional
            The maximum number of rows per group (default is 10,000).
        batch_size : int, optional
            The size of the batches to process. If None, the entire dataset is processed at once.
        **kwargs : dict, optional
            Additional arguments passed to the `pq.write_dataset` function.

        Example
        -------
        >>> db.optimize_dataset(max_rows_per_file=5000, batch_size=100)
        """
        logger.info("Optimizing dataset")

        # Read the entire dataset either in batches or as a whole
        schema=None
        if batch_size:
            table = self._load_data(output_format='batch_generator', batch_size=batch_size)
            schema=self.get_schema()
        else:
            table = self._load_data(output_format='table')
            
        
        # Backup current files in case of failure
        self._write_tmp_files()

        try:
            # Write optimized dataset
            pq.write_dataset(
                table,
                self.dataset_dir,
                basename_template=self.basename_template,
                schema=schema,
                format="parquet",
                max_rows_per_file=max_rows_per_file,
                min_rows_per_group=min_rows_per_group,
                max_rows_per_group=max_rows_per_group,
                existing_data_behavior='overwrite_or_ignore'
                **kwargs
            )
            logger.info(f"Optimized table {self.dataset_name}.")
        except Exception as e:
            logger.exception(f"exception optimizing table {self.dataset_name}: {e}")
            logger.info("Restoring original files")
            self._restore_tmp_files()

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
        table = self._load_data(output_format='table')
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
                                   batch_size: int = None, 
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
        batch_size : int, optional
            The size of the batches to process. If None, the entire dataset is processed at once.
        **kwargs : dict, optional
            Additional arguments passed to the `pq.write_to_dataset` function.

        Example
        -------
        >>> db.export_partitioned_dataset(export_dir='/path/to/export', partitioning={'year': '2023'}, partitioning_flavor='hive')
        """
        logger.info(f"Exporting partitioned dataset to {export_dir}")
        # Read the entire dataset either in batches or as a whole
        schema=None
        if batch_size:
            table = self._load_data( output_format='batch_generator', batch_size=batch_size)
            schema=self.get_schema()
        else:
            table = self._load_data(output_format='table')

        pq.write_to_dataset(
                table,
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
        self.create(data=df.to_dict(orient='records'))
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

    def _load_data(self, columns:List[str]=None, 
                   filter:List[pc.Expression]=None, 
                   batch_size:int=None, 
                   output_format:str='table',
                   load_tmp:bool=False,
                   load_kwargs:dict=None):
        """
        Loads data from the dataset, supporting various output formats such as PyArrow Table, Dataset, or a batch generator.

        Parameters
        ----------
        columns : list of str, optional
            A list of column names to load. If None, all columns are loaded (default is None).
        filter : list of pyarrow.compute.Expression, optional
            A list of filters to apply to the data (default is None).
        batch_size : int, optional
            The batch size to use for loading data in batches. If None, data is loaded as a whole (default is None).
        output_format : str, optional
            The format for loading the data: 'table', 'batch_generator', or 'dataset' (default is 'table').
        load_tmp : bool, optional
            If True, loads data from the temporary directory (default is False).
        load_kwargs : dict, optional
            Additional keyword arguments passed to `Dataset.to_table` or `Dataset.to_batches` (default is None).

        Returns
        -------
        Union[pa.Table, pa.dataset.Scanner, Iterator[pa.RecordBatch]]
            The loaded data as a PyArrow Table, Dataset, or batch generator, depending on the specified output format.

        Example
        -------
        >>> data = db._load_data(columns=['name', 'age'], output_format='table')
        """
        if load_kwargs is None:
            load_kwargs={}
            
        dataset_dir=self.dataset_dir
        if load_tmp:
            dataset_dir=self.tmp_dir
            
        logger.info(f"Loading data from {dataset_dir}")
        logger.info(f"Loading only columns: {columns}")
        logger.info(f"Using filter: {filter}")

        dataset = ds.dataset(dataset_dir, format="parquet")
        if output_format=='batch_generator':
            return self._load_batches(dataset, batch_size, columns, filter, **load_kwargs)
        elif output_format=='table':
            return self._load_table(dataset, columns, filter, **load_kwargs)
        elif output_format=='dataset':
            logger.info(f"Loading data as an {dataset.__class__} object")
            return dataset
        else:
            raise ValueError(f"output_format must be one of the following: {self.output_formats}")
    
    def _load_batches(self, dataset, batch_size, columns:List[str]=None, filter:List[pc.Expression]=None, **kwargs):
        """
        Loads data in batches from the dataset, returning an iterator of PyArrow RecordBatches.

        Parameters
        ----------
        dataset : pa.dataset.Dataset
            The PyArrow dataset from which to load data.
        batch_size : int
            The size of each batch to load.
        columns : list of str, optional
            A list of column names to load. If None, all columns are loaded (default is None).
        filter : list of pyarrow.compute.Expression, optional
            A list of filters to apply to the data (default is None).
        **kwargs : dict, optional
            Additional keyword arguments passed to `Dataset.to_batches`.

        Returns
        -------
        Iterator[pa.RecordBatch]
            An iterator yielding batches of data as PyArrow RecordBatches.

        Example
        -------
        >>> batches = db._load_batches(dataset, batch_size=100, columns=['name', 'age'])
        """
        if batch_size is None:
            raise ValueError("batch_size must be provided when output_format is batch_generator")
        try:
            generator=dataset.to_batches(columns=columns,filter=filter, batch_size=batch_size, **kwargs)
            logger.info(f"Loading as a {generator.__class__} object")
        except Exception as e:
            logger.debug(f"Error loading table: {e}. Returning empty table")
            generator=pyarrow_utils.create_empty_batch_generator(schema=dataset.schema, columns=columns)
        return generator
    
    def _load_table(self, dataset, columns:List[str]=None, filter:List[pc.Expression]=None, **kwargs):
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
        **kwargs : dict, optional
            Additional keyword arguments passed to `Dataset.to_table`.

        Returns
        -------
        pa.Table
            The loaded data as a PyArrow Table.

        Example
        -------
        >>> table = db._load_table(dataset, columns=['name', 'age'])
        """
        try:
            table=dataset.to_table(columns=columns,filter=filter,**kwargs)
            logger.info(f"Loading data as a {table.__class__} object")
        except Exception as e:
            logger.debug(f"Error loading table: {e}. Returning empty table")
            table=pyarrow_utils.create_empty_table(schema=dataset.schema, columns=columns)
        return table
    
    @timeit
    def _write_tmp_files(self, tmp_dir=None):
        """
        Copy current dataset files to a temporary directory.

        This method removes any existing files in the temporary directory and creates new ones by copying 
        the current dataset files. This is done to safeguard the original data while performing operations 
        that may overwrite or modify the dataset.

        Parameters
        ----------
        tmp_dir : str, optional
            The path to the temporary directory where the dataset files will be copied. 
            If not provided, it defaults to `self.tmp_dir`.

        Returns
        -------
        None
            This function does not return anything but creates a copy of the dataset files in the specified 
            temporary directory.
        """
        logger.info("Writing temporary files")
        if tmp_dir is None:
            tmp_dir=self.tmp_dir
        shutil.rmtree(self.tmp_dir)
        os.makedirs(self.tmp_dir, exist_ok=True)

        current_filepaths=glob(os.path.join(self.dataset_dir,f'{self.dataset_name}_*.parquet'))
        for i_file, current_filepath in enumerate(current_filepaths):
            basename=os.path.basename(current_filepath)
            
            tmp_filepath = os.path.join(self.tmp_dir, basename)
            shutil.copyfile(current_filepath, tmp_filepath)
        
        if os.path.exists(self.dataset_dir):
            shutil.rmtree(self.dataset_dir)
        os.makedirs(self.dataset_dir, exist_ok=True)
        logger.info("Temporary files written")
            
    @timeit
    def _restore_tmp_files(self, tmp_dir=None):
        """
        Restore temporary Parquet files from a given directory to the dataset directory.

        This function moves temporary Parquet files created during dataset operations
        from the temporary directory back to the dataset directory. It first identifies
        all Parquet files matching the dataset name pattern in the temporary directory,
        then copies them to the dataset directory, and finally deletes the original temporary files.

        Parameters
        ----------
        tmp_dir : str, optional
            The directory containing the temporary Parquet files. If not provided, it defaults
            to `self.tmp_dir`.

        Returns
        -------
        None
            This function does not return any value. It performs file operations by copying
            and deleting temporary files.

        """
        logger.info("Restoring temporary files")
        if tmp_dir is None:
            tmp_dir=self.tmp_dir
 
        tmp_filepaths=glob(os.path.join(self.tmp_dir,f'{self.dataset_name}_*.parquet'))
        for i_file, tmp_filepath in enumerate(tmp_filepaths):
            basename=os.path.basename(tmp_filepath)
            current_filepath = os.path.join(self.dataset_dir, basename)
            shutil.copyfile(tmp_filepath, current_filepath)
            os.remove(tmp_filepath)
        logger.info("Temporary files restored")
    
    def _validate_data(self, data):
        """
        Validate and convert input data into a list of dictionaries.

        This function checks the type of input data and converts it into a list of dictionaries
        if necessary. It supports input data as a dictionary, list of dictionaries, or a 
        pandas DataFrame. If the input is a dictionary, it is wrapped in a list. If the input
        is a pandas DataFrame, it is converted into a list of dictionaries. If the input is
        already a list, it is returned as-is. Raises a `TypeError` for unsupported data types.

        Parameters
        ----------
        data : dict, list, pd.DataFrame, or None
            The input data to validate and convert. Supported types are:
            - dict: A single dictionary will be wrapped in a list.
            - list: A list of dictionaries will be returned as-is.
            - pd.DataFrame: Converted to a list of dictionaries.
            - None: Returns `None`.

        Returns
        -------
        list or None
            A list of dictionaries if valid data is provided, otherwise `None` if `data` is None.
        """
        logger.info("Validating data")
        if isinstance(data, dict):
            data_list = [data]
        elif isinstance(data, list):
            data_list = data
        elif isinstance(data, pd.DataFrame):
            data_list = data.to_dict(orient='records')
        elif data is None:
            data_list = None
        else:
            raise TypeError("Data must be a dictionary or a list of dictionaries.")
        
        # Convert to pyarrow array to get the schema
        struct=pa.array(data_list)
        schema=pa.schema(struct.type)
        logger.info("Data validated")
        return data_list, schema
    
    def _get_new_ids(self, data_list:List[dict]):
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
            table = self._load_data(columns=['id'],output_format='table')
            max_val=pc.max(table.column('id')).as_py()
            start_id = max_val + 1  # Start from the next available ID
            
            logger.debug(f"Directory is not empty. Starting id from {start_id}")
    
        # Create a list of new IDs
        new_ids = list(range(start_id, start_id + len(data_list)))
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
        current_table=self.read(columns=['id'], output_format='table').combine_chunks()
        filtered_table = current_table.filter(~pc.field('id').isin(id_column))

        if filtered_table.num_rows==0:
            logger.warning(f"The following ids are not in the main table", extra={'ids_do_not_exist': filtered_table['id'].combine_chunks()})
        return None


def generator_schema_cast(generator, new_schema):
    for record_batch in generator:
        updated_record_batch=pyarrow_utils.table_schema_cast(record_batch, new_schema)
        yield updated_record_batch
        
def table_schema_cast(current_table, new_schema):
    updated_table=pyarrow_utils.table_schema_cast(current_table, new_schema)
    return updated_table
        
        
def table_update(current_table, incoming_table):
    current_table = pyarrow_utils.table_schema_cast(current_table, incoming_table.schema)
    updated_record_batch=pyarrow_utils.update_flattend_table(current_table, incoming_table)
    return updated_record_batch
        
def generator_update(generator, incoming_table):
    for record_batch in generator:
        updated_record_batch=pyarrow_utils.update_flattend_table(record_batch, incoming_table)
        yield updated_record_batch
        
def table_delete(current_table, ids):
    updated_table = current_table.filter( ~pc.field('id').isin(ids) )
    return updated_table

def generator_delete(generator, ids):
    for record_batch in generator:
        updated_record_batch = record_batch.filter( ~pc.field('id').isin(ids) )
        yield updated_record_batch