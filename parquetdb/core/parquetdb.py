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

from parquetdb.utils.general_utils import timeit, is_directory_empty
from parquetdb.utils import pyarrow_utils

# Logger setup
logger = logging.getLogger(__name__)


#TODO: Add option to skip create aligment if the schema is the exact same
#TODO: Need better method to check if schema is the same and identify mismatch
#TODO: Need to remove raw reads and writes of tables in create, update, and delete methods. 
# This can cause issue if the original table is large. I need to adapt the method to handle batches instead.

def get_field_names(filepath, columns=None, include_cols=True):
    """
    Gets the field names from a parquet file.

    Args:
        filepath (str): The path to the parquet file.
        columns (list): A list of column names to include.
        include_cols (bool): If True, includes all columns except the ones listed in columns.

    Returns:
        list: A list of column names.
    """
    if not include_cols:
        metadata = pq.read_metadata(filepath)
        all_columns = []
        for filed_schema in metadata.schema:
            
            # Only want top column names
            max_defintion_level=filed_schema.max_definition_level
            if max_defintion_level!=1:
                continue

            all_columns.append(filed_schema.name)

        columns = [col for col in all_columns if col not in columns]
    return columns

class ParquetDB:
    def __init__(self, dataset_name, dir='', n_cores=8):
        """
        Initializes the ParquetDBDataset object.

        Args:
            db_path (str): The path to the root directory of the database.
            n_cores (int): The number of CPU cores to be used for parallel processing.
        """
        self.dir = dir
        self.dataset_name=dataset_name
        self.dataset_dir=os.path.join(self.dir,self.dataset_name)
        self.tmp_dir=os.path.join(self.dir,'tmp')
        self.n_cores = n_cores
        self.basename_template = f'{dataset_name}_{{i}}.parquet'


        os.makedirs(self.tmp_dir, exist_ok=True)
        os.makedirs(self.dataset_dir, exist_ok=True)

        
        self.output_formats=['batch_generator','table','dataset']
        self.reserved_dataset_names=['tmp']

        logger.info(f"dir: {self.dir}")
        logger.info(f"reserved_dataset_names: {self.reserved_dataset_names}")
        logger.info(f"n_cores: {self.n_cores}")
        logger.info(f"output_formats: {self.output_formats}")

    @timeit
    def create(self, data:Union[List[dict],dict,pd.DataFrame],
               batch_size:int=None,
               schema=None,
               metadata=None,
               normalize_dataset:bool=True,
               normalize_kwagrs:dict=dict(max_rows_per_file=100000,
                                        min_rows_per_group=0,
                                        max_rows_per_group=100000)
               ):
        """
        Adds new data to the database.

        Args:
            data (dict or list of dicts): The data to be added to the database. 
                This must contain
            batch_size (int): The batch size. 
                If provided, create will return a generator that yields batches of data.
            schema (pyarrow.Schema): The schema of the incoming table.
            metadata (dict): Metadata to be added to the table.
            normalize (bool): Whether to normalize the dataset or not. The default is True.
                This means after the reconsoling the schema adding the incoming data to its own parquet table.
                The dataset files will be recreated to have an equal number of rows 
                as defined by the Dataset.write_dataset function. This doubles write speed times. 
                The better method is to have this be off and then normalize after the data ingestion.
            normalize_kwagrs (dict): Additional keyword arguments to pass to the normalize function.
        """
        
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
        # Sometimes records have a  nested dictionaries and some do not. 
        # This ensures all records have the same nested structes
        incoming_table=pyarrow_utils.fill_null_nested_structs_in_table(incoming_table)
        incoming_table=pyarrow_utils.replace_empty_structs_in_table(incoming_table)
        
        incoming_schema=incoming_table.schema
        
        # If this is the first table, save it directly
        if is_directory_empty(self.dataset_dir):
            incoming_save_path = self._get_save_path()
            pq.write_table(incoming_table, incoming_save_path)
            return None

        # Write tmp files for the original table
        # This is done to ensure that the original table is not deleted 
        # if there is an exception during the writing process
        self._write_tmp_files()

        # Align the incoming and original schemas
        try:
            current_schema = self.get_schema()
            
            # merged_schema=pyarrow_utils.merge_schemas(current_schema, incoming_schema)
            merged_schema = pa.unify_schemas([current_schema, incoming_schema],promote_options='permissive')
            are_schemas_equal=current_schema.equals(incoming_schema)
        
            logger.info(f"Schemas are equal: {are_schemas_equal}. Skiping schema alignment.")
            if not are_schemas_equal:
                logger.debug(f"\n\n{merged_schema}\n\n")
                # Align file schemas with merged schema
                current_files = glob(os.path.join(self.dataset_dir, f'{self.dataset_name}_*.parquet'))
                for current_file in current_files:
                    # This might inefficient because I convert the table to a pylist 
                    # and then back to a table with the merged schema
                    # This was done because this will automatically fill non-exisiting 
                    # fields in the current table if the update creates new fields
                    current_table = pq.read_table(current_file)
                    current_table=pa.Table.from_pylist(current_table.to_pylist(), schema=merged_schema)
                    # Sometimes records have a  nested dictionaries and some do not. 
                    # This ensures all records have the same nested structes
                    current_table=pyarrow_utils.fill_null_nested_structs_in_table(current_table)

                    pq.write_table(current_table, current_file)

                incoming_table=incoming_table.to_pylist()
                incoming_table=pa.Table.from_pylist(data_list, schema=merged_schema)
                # incoming_table=pyarrow_utils.align_table(incoming_table, merged_schema)

        except Exception as e:
            logger.exception(f"exception aligning schemas: {e}")
            logger.info("Restoring original files")
            self._restore_tmp_files()
        
        # Save the incoming table
        incoming_save_path = self._get_save_path()
        pq.write_table(incoming_table, incoming_save_path)

        if normalize_dataset:
            self.normalize(schema=incoming_table.schema, batch_size=batch_size, **normalize_kwagrs)
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
            load_kwargs (dict): Additional keyword arguments to pass to Dataset.to_table or Dataset.to_batches.

        Returns:
            pandas.DataFrame or list: The data read from the database. If deserialize_data is True,
            returns a list of dictionaries with their 'id's. Otherwise, returns the DataFrame with serialized data.
        """

        # Check if the table name is in the list of table names
        table_path=os.path.join(self.dataset_dir, f'{self.dataset_name}.parquet')

        if columns:
            columns=get_field_names(table_path, columns=columns, include_cols=include_cols)

        if filters is None:
            filters = []
            
        if batch_size:
            logger.info(f"Found batch_size: {batch_size}. Setting output_format to batch_generator")
            output_format='batch_generator'
            
        # Build filter expression
        filter_expression = self._build_filter_expression(ids, filters)

        data = self._load_data(columns=columns, filter=filter_expression, 
                               batch_size=batch_size, output_format=output_format, load_kwargs=load_kwargs)
        return data
    
    @timeit
    def update(self, data: Union[List[dict], dict, pd.DataFrame], field_type_dict=None):
        """
        Updates data in the database.

        Args:
            data (dict or list of dicts or pandas.DataFrame): The data to be updated.
                Each dict should have an 'id' key corresponding to the record to update.
            field_type_dict (dict): A dictionary where the keys are the field names and the values are the new field types.

            **kwargs: Additional keyword arguments.

        Raises:
            ValueError: If new fields are found in the update data that do not exist in the schema.
        """

        # Data processing and validation.
        data_list, incoming_schema = self._validate_data(data)

        
        # Schema validation steps
        current_schema=self.get_schema()
        merged_schema = pa.unify_schemas([current_schema, incoming_schema],promote_options='default')
        
        incoming_table=pa.Table.from_pylist(data_list, schema=merged_schema)
        
        # Non-exisiting id warning step. THis is not really necessary but might be nice for user to check
        self._validate_id(incoming_table['id'].combine_chunks())
        
        # Sometimes records have a  nested dictionaries and some do not. 
        # This ensures all records have the same nested structes
        incoming_table=pyarrow_utils.fill_null_nested_structs_in_table(incoming_table)
        
        # This ensures empty structs/dicts are not empty, it fills it with dummy varaible. 
        # This is important because saving will raise an issue if they are empty
        incoming_table=pyarrow_utils.replace_empty_structs_in_table(incoming_table)
        
        # Iterate over the original files
        self._write_tmp_files()
        current_files=self.get_current_files()
        for i_file, current_file in enumerate(current_files):
            # Opening a table with a merged schema will add missing values to the table
            
            try:
                # This might inefficient because I convert the table to a pylist 
                # and then back to a table with the merged schema
                # This was done because this will automatically fill non-exisiting 
                # fields in the current table if the update creates new fields
                current_table = pq.read_table(current_file)
                current_table=pa.Table.from_pylist(current_table.to_pylist(), schema=merged_schema)
                current_table=pyarrow_utils.fill_null_nested_structs_in_table(current_table)
                
                # The flatten method will flatten out all nested structs, update, then rebuild the nested structs
                updated_table=pyarrow_utils.update_table(current_table, incoming_table, flatten_method=True)
            except Exception as e:
                logger.exception(f"exception updating {current_file}")
                # logger.exception(f"exception updating {current_file}: {e}")
                # If something goes wrong, restore the original file
                self._restore_tmp_files()
                break

            pq.write_table(updated_table, current_file)
            logger.info(f"Updated {current_file} with {updated_table.shape}")
        logger.info(f"Updated {self.dataset_name} table.")

    @timeit
    def delete(self, ids:List[int]):
        """
        Deletes data from the database.

        Args:
            ids (list): A list of IDs to delete.

        Returns:
            None
        """
        logger.info("Deleting data from the database")
        ids=set(ids)

        # Check if any of the IDs to delete exist in the table. If not, return None
        current_id_table=self._load_data(columns=['id'], output_format='table')
        filtered_id_table = current_id_table.filter( pc.field('id').isin(ids) )
        if filtered_id_table.num_rows==0:
            logger.info(f"No data found to delete.")
            return None
        
        # Dataset directory
        logger.info(f"Dataset directory: {self.dataset_dir}")
        
        # Backup current files in case of failure
        self._write_tmp_files()
        current_files = self.get_current_files()
        for current_file in current_files:
            filename=os.path.basename(current_file)

            try:
                current_table=pq.read_table(current_file)
                updated_table = current_table.filter( ~pc.field('id').isin(ids) )
            except Exception as e:
                logger.exception(f"exception processing {current_file}: {e}")
                # If something goes wrong, restore the original file
                self._restore_tmp_files()
                raise e
            
            # Saving the new table with the deleted IDs
            pq.write_table(updated_table, current_file)

            n_rows_deleted=current_table.shape[0]-updated_table.shape[0]
            logger.info(f"Deleted {n_rows_deleted} Indices from {filename}. The shape is now {updated_table.shape}")

        logger.info(f"Updated {self.dataset_name} table.")

    @timeit
    def normalize(self, schema=None, batch_size: int = None, load_kwargs: dict = None, output_format: str = 'table',
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
        elif output_format=='table':
            schema=None

        try:
            dataset = self._load_data(batch_size=batch_size, output_format=output_format, load_tmp=True, load_kwargs=load_kwargs)
        except pa.lib.ArrowNotImplementedError as e:
            raise ValueError("The incoming data does not match the schema of the existing data.") from e

        try:
            logger.info(f"Writing final table to {self.dataset_dir}")
            shutil.rmtree(self.dataset_dir)
            
            ds.write_dataset(dataset, self.dataset_dir, basename_template=self.basename_template, schema=schema,
                            format="parquet", max_partitions=kwargs.get('max_partitions', 1024),
                            max_open_files=kwargs.get('max_open_files', 1024), max_rows_per_file=max_rows_per_file, 
                            min_rows_per_group=min_rows_per_group, max_rows_per_group=max_rows_per_group,
                            existing_data_behavior=existing_data_behavior, **kwargs)
            
        except Exception as e:
            logger.exception(f"exception writing final table to {self.dataset_dir}: {e}")
            logger.info("Restoring original files")
            self._restore_tmp_files()

    @timeit
    def update_schema(self, field_dict:dict=None, schema:pa.Schema=None):
        """
        Updates the schema of the table.

        Args:
            dataset_name (str): The name of the table to update the schema of.
            field_dict (dict): A dictionary where the keys are the field names and the values are the new field types.
            schema (pyarrow.Schema): The new schema for the table.

        """
        # Dataset directory
        current_files=self.get_current_files()

        # Backup current files in case of failure
        self._write_tmp_files()

        for current_file in current_files:
            filename=os.path.basename(current_file)
            current_table=pq.read_table(current_file)

            updated_table=self._update_dataset_schema(current_table, schema, field_dict)

            try:
                pq.write_table(updated_table, current_file)
            except Exception as e:
                logger.exception(f"exception processing {current_file}: {e}")
                # If something goes wrong, restore the original file
                self._restore_tmp_files()
                break

            logger.info(f"Updated {filename} with {current_table.shape}")

        logger.info(f"Updated Fields in {self.dataset_name} table.")

    def get_schema(self, load_tmp=False):
        """Get the schema of a table.

        Args:
            dataset_name (str): The name of the table.

        Returns:
            pyarrow.Schema: The schema of the table.
        """
        schema = self._load_data(output_format='dataset', load_tmp=load_tmp).schema
        return schema
    
    def get_metadata(self):
        """Get the metadata of a table.
        
        Args:
            dataset_name (str): The name of the table.

        Returns:
            dict: The metadata of the table.
        """
        if not self.dataset_exists():
            raise ValueError(f"Dataset {self.dataset_name} does not exist.")
        schema = self.get_schema()
        logger.debug(f"Metadata:\n\n {schema.metadata}\n\n")
        return schema.metadata
    
    def set_metadata(self, metadata:dict):
        """Set the metadata of a table.

        Args:
            dataset_name (str): The name of the table.
            metadata (dict): The metadata to set.
        """
        # Update metadata in schema and rewrite Parquet files
        self.update_schema(schema=pa.schema(self.get_schema().fields, metadata=metadata))

    def get_current_files(self):
        """Gets the current files in the dataset directory.

        Returns
        -------
        List[str]
            A list of file paths for the current dataset files.
        """
        return glob(os.path.join(self.dataset_dir, f'{self.dataset_name}_*.parquet'))
    
    def dataset_exists(self, dataset_name:str=None):
        """Checks if a table exists.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        
        if dataset_name:
            dataset_dir=os.path.join(self.dir,dataset_name)
            return os.path.exists(dataset_dir)
        else:
            return len(self.get_current_files()) != 0

    def drop_dataset(self):
        """
        Drops a table. by removing the table directory.

        Args:
            dataset_name (str): The name of the table to drop.
        """
      
        if os.path.exists(self.dataset_dir):
            shutil.rmtree(self.dataset_dir)
            logger.info(f"Table {self.dataset_name} has been dropped.")
        else:
            logger.warning(f"Table {self.dataset_name} does not exist.")
    
    def rename_dataset(self, new_name:str):
        """
        Renames a table.

        Args:
            old_name (str): The current name of the table.
            new_name (str): The new name of the table.
        """
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
        Copies a table to a new table.

        Args:
            source_table (str): The name of the source table.
            dest_table (str): The name of the destination table.
        """
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
        Optimizes the table by merging small Parquet files.

        Args:
            dataset_name (str): The name of the table to optimize.
            max_rows_per_file (int): The maximum number of rows per file.
            min_rows_per_group (int): The minimum number of rows per group.
            max_rows_per_group (int): The maximum number of rows per group.
            batch_size (int): The batch size.
            **kwargs: Additional keyword arguments to pass to the pq.write_to_dataset function.
        """

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
                min_rows_per_group=0,
                max_rows_per_group=10000,
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
        Exports the table to a specified file format.

        Args:
            dataset_name (str): The name of the table to export.
            file_path (str): The file path to export the data to.
            format (str): The format to export ('csv', 'json').
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
        This method exports a partitioned dataset to a specified file format.

        Args:
            dataset_name (str): The name of the table to export.
            export_dir (str): The directory to export the data to.
            partitioning (dict): The partitioning to use for the dataset.
            partitioning_flavor (str): The partitioning flavor to use.
            batch_size (int): The batch size.
            **kwargs: Additional keyword arguments to pass to the pq.write_to_dataset function.

        """

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

    def import_dataset(self, file_path: str, format: str = 'csv', **kwargs):
        """
        Imports a table from a specified file format.

        Args:
            file_path (str): The file path to import the data from.
            dataset_name (str): The name of the table to import the data into.
            format (str): The format to import ('csv', 'json').
            **kwargs: Additional keyword arguments to pass to the create function.
        """
        if format == 'csv':
            df = pd.read_csv(file_path, **kwargs)
        elif format == 'json':
            df = pd.read_json(file_path, **kwargs)
        else:
            raise ValueError(f"Unsupported import format: {format}")
        self.create(data=df.to_dict(orient='records'))
        logger.info(f"Imported data from {file_path} into table {self.dataset_name}.")

    def merge_datasets(self, source_tables: List[str], dest_table: str):
        raise NotImplementedError
    
    def backup_database(self, backup_path: str):
        """
        Creates a backup of the database.

        Args:
            backup_path (str): The path where the backup will be stored.
        """
        shutil.copytree(self.dataset_dir, backup_path)
        logger.info(f"Database backed up to {backup_path}.")

    def restore_database(self, backup_path: str):
        """
        Restores the database from a backup.

        Args:
            backup_path (str): The path to the backup to restore from.
        """
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
        This method loads the data in the database. It can either load the data as a PyArrow Table, PyArrow Dataset, PyArrow generator.
        
        Parameters
        ----------
        
        columns : List[str], optional
            A list of column names to load. If None, loads all columns. Defaults to None.
        filter : List[pc.Expression], optional
            A list of filters to apply to the data. Defaults to None.
        batch_size : int, optional
            The batch size to use for loading data in batches. Defaults to None.
        output_format : str, optional
            The output format to use for loading data. Defaults to 'table'.
        load_tmp : bool, optional
            Whether to load data from the temporary directory. Defaults to False.
        load_kwargs : dict, optional
            Additional keyword arguments to pass to Dataset.to_table or Dataset.to_batches. Defaults to None.

        Returns
        -------
        Union[pa.Table, pa.dataset.Scanner]
            The loaded data as a PyArrow Table, PyArrow Dataset, or PyArrow generator.
        
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
        try:
            table=dataset.to_table(columns=columns,filter=filter,**kwargs)
            logger.info(f"Loading data as a {table.__class__} object")
        except Exception as e:
            logger.debug(f"Error loading table: {e}. Returning empty table")
            table=pyarrow_utils.create_empty_table(schema=dataset.schema, columns=columns)
        return table
    
    def _get_new_ids(self, data_list:List[dict]):
  
        if is_directory_empty(self.dataset_dir):
            start_id = 0
        else:
            table = self._load_data(columns=['id'],output_format='table')
            max_val=pc.max(table.column('id')).as_py()
            start_id = max_val + 1  # Start from the next available ID
    
        # Create a list of new IDs
        new_ids = list(range(start_id, start_id + len(data_list)))
        return new_ids
    
    def _build_filter_expression(self, ids: List[int], filters: List[pc.Expression]):
        """Helper function to build the filter expression."""
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

        return filter_expression

    def _get_save_path(self):
        """Determine the path to save the incoming table."""

        n_files = len(glob(os.path.join(self.dataset_dir, f'{self.dataset_name}_*.parquet')))
        
        if n_files == 0:
            return os.path.join(self.dataset_dir, f'{self.dataset_name}_0.parquet')
        return os.path.join(self.dataset_dir, f'{self.dataset_name}_{n_files}.parquet')

    def _validate_id(self, id_column):
        logger.info(f"Validating ids")
        current_table=self.read(columns=['id'], output_format='table').combine_chunks()
        filtered_table = current_table.filter(~pc.field('id').isin(id_column))
        logger.warning(f"The following ids are not in the main table", extra={'ids_do_not_exist': filtered_table['id'].to_pylist()})
        return None
    
    def _update_dataset_schema(self, current_table, schema=None, field_dict=None):
        """
        Update the schema of a given table based on a provided schema or field modifications.

        This function allows updating the schema of a PyArrow table by either replacing the entire schema 
        or modifying individual fields within the existing schema. It can take a dictionary of field 
        names and their corresponding new field definitions to update specific fields in the schema.
        Alternatively, a completely new schema can be provided to replace the current one.

        Parameters
        ----------
        current_table : pa.Table
            The PyArrow table whose schema is to be updated.
        
        schema : pa.Schema, optional
            A new schema to replace the existing schema of the table. If provided, this will
            completely override the current schema.
        
        field_dict : dict, optional
            A dictionary where the keys are existing field names and the values are the new
            PyArrow field definitions to replace the old ones. This is used for selectively 
            updating specific fields within the current schema.

        Returns
        -------
        pa.Table
            A new PyArrow table with the updated schema.
        """
        # Check if the table name is in the list of table names

        current_schema=current_table.schema
        current_field_names=current_table.column_names

        if field_dict:
            updated_schema=current_schema
            for field_name, new_field in field_dict.items():
                field_index=current_schema.get_field_index(field_name)

                if field_name in current_field_names:
                    updated_schema=updated_schema.set(field_index, new_field)

        if schema:
            updated_schema=schema

        pylist=current_table.to_pylist()
        new_table=pa.Table.from_pylist(pylist, schema=updated_schema)

        return new_table

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
        if tmp_dir is None:
            tmp_dir=self.tmp_dir
        shutil.rmtree(self.tmp_dir)
        os.makedirs(self.tmp_dir, exist_ok=True)

        current_filepaths=glob(os.path.join(self.dataset_dir,f'{self.dataset_name}_*.parquet'))
        for i_file, current_filepath in enumerate(current_filepaths):
            basename=os.path.basename(current_filepath)
            
            tmp_filepath = os.path.join(self.tmp_dir, basename)
            shutil.copyfile(current_filepath, tmp_filepath)
            
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
        if tmp_dir is None:
            tmp_dir=self.tmp_dir
 
        tmp_filepaths=glob(os.path.join(self.tmp_dir,f'{self.dataset_name}_*.parquet'))
        for i_file, tmp_filepath in enumerate(tmp_filepaths):
            basename=os.path.basename(tmp_filepath)
            current_filepath = os.path.join(self.dataset_dir, basename)
            shutil.copyfile(tmp_filepath, current_filepath)
            os.remove(tmp_filepath)
    
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
        return data_list, schema
    

def add_dummy_field_to_empty_structs(table: pa.Table, dummy_field_name="dummy_field", dummy_field_type=pa.int32()) -> pa.Table:
    """
    Search for struct types with no child fields and add a dummy field to those structs.

    Parameters:
    - table: A PyArrow Table to check for empty struct types.
    - dummy_field_name: Name of the dummy field to add.
    - dummy_field_type: DataType of the dummy field (default is pa.int32).

    Returns:
    - A new PyArrow Table with the updated schema.
    """
    schema = table.schema
    new_fields = []

    # Traverse the schema fields
    for field in schema:
        if pa.types.is_struct(field.type):
            struct_type = field.type
            if len(struct_type) == 0:
                # Empty struct found, add a dummy field to it
                new_struct_type = pa.struct([pa.field(dummy_field_name, dummy_field_type)])
                new_fields.append(pa.field(field.name, new_struct_type))
            else:
                # Keep the existing struct field with its children
                new_fields.append(field)
        else:
            # Keep non-struct fields as is
            new_fields.append(field)

    # Create new schema with updated fields
    new_schema = pa.schema(new_fields)

    # Reconstruct the table with the updated schema
    # Assuming you can reconstruct the data based on new schema
    return table.cast(new_schema)

def deep_update(original_value, update_value):
    """
    Recursively updates original_value with the corresponding values in update_value.
    If update_value contains nested dictionaries or lists, it updates them recursively.
    
    Args:
        original_value: The original value to be updated (could be a dict, list, or primitive).
        update_value: The update value (could be a dict, list, or primitive).
    
    Returns:
        Updated original_value with changes from update_value.
    """
    # If both are dictionaries, update recursively
    if isinstance(original_value, dict) and isinstance(update_value, dict):
        for key, value in update_value.items():
            if key in original_value:
                original_value[key] = deep_update(original_value[key], value)
            else:
                original_value[key] = value
        return original_value
    
    # # If both are lists, update recursively by index
    # elif isinstance(original_value, list) and isinstance(update_value, list):
    #     # Extend the original list if update_value has more items
    #     for i in range(len(update_value)):
    #         if i < len(original_value):
    #             original_value[i] = deep_update(original_value[i], update_value[i])
    #         else:
    #             original_value.append(update_value[i])
    #     return original_value
    
    # Otherwise, just return the update value (for primitive types or if the types differ)
    return update_value
