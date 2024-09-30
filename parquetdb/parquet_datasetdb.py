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

from parquetdb.utils import timeit, is_directory_empty
from parquetdb.pyarrow_utils import combine_tables, merge_schemas, align_table,replace_none_with_nulls

# Logger setup
logger = logging.getLogger(__name__)

# TODO:  If a dictionary is empty there will be an error

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

class ParquetDatasetDB:
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
               max_rows_per_file=10000,
               min_rows_per_group=0,
               max_rows_per_group=10000,
               schema=None,
               metadata=None,
               **kwargs):
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
        
        os.makedirs(self.dataset_dir, exist_ok=True)
        
        # Prepare the data and field data
        data_list=self._validate_data(data)
        new_ids = self._get_new_ids( data_list)

        # Create the incoming table
        incoming_table=pa.Table.from_pylist(data_list, schema=schema, metadata=metadata)
        incoming_table=incoming_table.append_column(pa.field('id', pa.int64()), [new_ids])

        # If this is the first table, save it directly
        if is_directory_empty(self.dataset_dir):
            incoming_save_path = self._get_save_path()
            pq.write_table(incoming_table, incoming_save_path)
            return None

        # Write tmp files for the original table
        # This is done to ensure that the original table is not deleted 
        # if there is an error during the writing process
        self._write_tmp_files()

        # Align the incoming and original schemas
        try:
            # Merge schemas
            incoming_schema = incoming_table.schema
            current_schema = self.get_schema()
            merged_schema=merge_schemas(current_schema, incoming_schema)
        
            # Align file schemas with merged schema
            current_files = glob(os.path.join(self.dataset_dir, f'{self.dataset_name}_*.parquet'))
            for current_file in current_files:
                current_table = pq.read_table(current_file)

                # Align current schema to match incoming
                updated_table=align_table(current_table, merged_schema)
                pq.write_table(updated_table, current_file)

            # Align incoming tbale to match merged schema
            incoming_table=align_table(incoming_table, merged_schema)

        except Exception as e:
            logger.error(f"Error aligning schemas: {e}")
            logger.info("Restoring original files")
            self._restore_tmp_files()
        
        # Save the incoming table
        incoming_save_path = self._get_save_path()
        pq.write_table(incoming_table, incoming_save_path)

        self._finalize_dataset(incoming_table.schema, batch_size, max_rows_per_file, 
                                min_rows_per_group, max_rows_per_group, **kwargs)
        return None

    @timeit
    def read(
        self,
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

        # Check if the table name is in the list of table names
        table_path=os.path.join(self.dataset_dir, f'{self.dataset_name}.parquet')

        if columns:
            columns=get_field_names(table_path, columns=columns, include_cols=include_cols)

        if filters is None:
            filters = []
            
        # Build filter expression
        filter_expression = self._build_filter_expression(ids, filters)

        data = self._load_data(columns=columns, filter=filter_expression, 
                               batch_size=batch_size, output_format=output_format)
        return data
    
    @timeit
    def update(self, data: Union[List[dict], dict, pd.DataFrame], field_type_dict=None):
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

        # Data processing and validation.
        data_list = self._validate_data(data)

        # Process update data and get id_data_dict_map and update schema
        id_data_dict_map, update_schema = self._process_update_data(data_list, field_type_dict)
        
        logger.info(f"Dataset directory: {self.dataset_dir}")
        current_files=self._get_current_files()

        # Iterate over the original files
        self._write_tmp_files()

        for i_file, current_file in enumerate(current_files):
            current_table = pq.read_table(current_file)

            try:
                updated_table = self._update_table(current_table, id_data_dict_map, update_schema)
            except Exception as e:
                logger.error(f"Error updating {current_file}: {e}\n{traceback.format_exc()}")
                # logger.error(f"Error updating {current_file}: {e}")
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
            dataset_name (str): The name of the table to delete data from.

        Returns:
            None
        """
        ids=set(ids)

        logger.info(f"Deleting data from {self.dataset_name}")

        main_id_column=self._load_data(columns=['id'], output_format='table')

        # Check if any of the IDs to delete exist in the table
        filtered_table = main_id_column.filter( pc.field('id').isin(ids) )
        if filtered_table.num_rows==0:
            logger.info(f"No data found to delete.")
            return None
        
        # Dataset directory
        logger.info(f"Dataset directory: {self.dataset_dir}")
        current_files = self._get_current_files()

        # Backup current files in case of failure
        self._write_tmp_files()

        for current_file in current_files:
            filename=os.path.basename(current_file)

            current_table=pq.read_table(current_file)

            # Delete the IDs from the table
            try:
                updated_table = self._delete_ids_from_table(ids, current_table)
            except Exception as e:
                logger.error(f"Error processing {current_file}: {e}")
                # If something goes wrong, restore the original file
                self._restore_tmp_files()
                raise e
            
            # Saving the new table with the deleted IDs
            pq.write_table(updated_table, current_file)

            n_rows_deleted=current_table.shape[0]-updated_table.shape[0]
            logger.info(f"Deleted {n_rows_deleted} Indices from {filename}. The shape is now {updated_table.shape}")

        logger.info(f"Updated {self.dataset_name} table.")

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
        current_files=self._get_current_files()

        # Backup current files in case of failure
        self._write_tmp_files()

        for current_file in current_files:
            filename=os.path.basename(current_file)
            current_table=pq.read_table(current_file)

            updated_table=self._update_dataset_schema(current_table, schema, field_dict)

            try:
                pq.write_table(updated_table, current_file)
            except Exception as e:
                logger.error(f"Error processing {current_file}: {e}")
                # If something goes wrong, restore the original file
                self._restore_tmp_files()
                break

            logger.info(f"Updated {filename} with {current_table.shape}")

        logger.info(f"Updated Fields in {self.dataset_name} table.")

    def get_schema(self):
        """Get the schema of a table.

        Args:
            dataset_name (str): The name of the table.

        Returns:
            pyarrow.Schema: The schema of the table.
        """
        schema = self._load_data(output_format='dataset').schema
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
        return schema.metadata
    
    def set_metadata(self, metadata:dict):
        """Set the metadata of a table.

        Args:
            dataset_name (str): The name of the table.
            metadata (dict): The metadata to set.
        """
        # Update metadata in schema and rewrite Parquet files
        self.update_schema(schema=pa.schema(self.get_schema().fields, metadata=metadata))

    def dataset_exists(self, dataset_name:str=None):
        """Checks if a table exists.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        
        if dataset_name:
            dataset_dir=os.path.join(self.dir,dataset_name)
            return os.path.exists(dataset_dir)
        else:
            return len(self._get_current_files()) != 0

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
            logger.error(f"Error optimizing table {self.dataset_name}: {e}")
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
                   load_tmp:bool=False):
        """
        This method loads the data in the database. It can either load the data as a PyArrow Table, PyArrow Dataset, PyArrow generator.
        """
        dataset_dir=self.dataset_dir
        if load_tmp:
            dataset_dir=self.tmp_dir
        logger.info(f"Loading data from {dataset_dir}")
        logger.info(f"Loading only columns: {columns}")
        logger.info(f"Using filter: {filter}")

        dataset = ds.dataset(dataset_dir, format="parquet")
        if output_format=='batch_generator':
            if batch_size is None:
                raise ValueError("batch_size must be provided when output_format is batch_generator")
            logger.info(f"Loading in batches")
            return dataset.to_batches(columns=columns,filter=filter,batch_size=batch_size)
        elif output_format=='table':
            logger.info(f"Loading data in one table")
            return dataset.to_table(columns=columns,filter=filter)
        elif output_format=='dataset':
            return dataset
        else:
            raise ValueError(f"output_format must be one of the following: {self.output_formats}")
    
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
    
    def add_null_columns_for_missing_fields(self, table, new_schema):
        """
        This function adds null columns for any fields that are missing in the new schema.

        Args:
            table (pa.Table): The table to add null columns to.
            new_schema (pa.Schema): The schema of the new table.

        Returns:
            pa.Table: The table with null columns added for missing fields.
        """
        table_schema = table.schema
        field_names_missing = list(set(new_schema.names) - set(table_schema.names))
        for new_field_name in field_names_missing:
            field_type = new_schema.field(new_field_name).type
            null_array = pa.nulls(table.shape[0], type=field_type)
            table = table.append_column(new_field_name, null_array)
        return table
    
    def _finalize_dataset(self,
                        schema:pa.Schema=None,
                        batch_size:int=None, 
                        max_rows_per_file:int=10000, 
                        min_rows_per_group:int=0, 
                        max_rows_per_group:int=10000, 
                        **kwargs):
        """Finalize the creation by writing the data to disk."""

        output_format='table'
        if batch_size:
            output_format='batch_generator'
        else:
            schema=None

        self._write_tmp_files()
        final_table = self._load_data(batch_size=batch_size, output_format=output_format, load_tmp=True)

        try:
            logger.info(f"Writing final table to {self.dataset_dir}")
            shutil.rmtree(self.dataset_dir)
            
            ds.write_dataset(final_table, self.dataset_dir, basename_template=self.basename_template, schema=schema,
                            format="parquet", max_partitions=kwargs.get('max_partitions', 1024),
                            max_open_files=kwargs.get('max_open_files', 1024), max_rows_per_file=max_rows_per_file, 
                            min_rows_per_group=min_rows_per_group, max_rows_per_group=max_rows_per_group,
                            existing_data_behavior='overwrite_or_ignore')
            
        except Exception as e:
            logger.error(f"Error writing final table to {self.dataset_dir}: {e}")
            logger.info("Restoring original files")
            self._restore_tmp_files()

    def _process_update_data(self, data_list: List[dict], field_type_dict=None):
        """Process and validate incoming data, return update dictionary, incoming field names, and inferred types."""
        if field_type_dict is None:
            field_type_dict={}
        
        id_data_dict_map = {}
        incoming_field_names = set()
        infered_types=None
        
        main_id_column=self._load_data(columns=['id'], 
                                       output_format='table')['id'].to_pylist()
        # Create id_data_dict_map, get incoming field names, and their infered types
        for data_dict in data_list:
            data_id = data_dict.get('id', None)

            # Check if the id is in the main table
            self._validate_id(data_id, main_id_column)

            # Add data_dict to id_data_dict_map
            id_data_dict_map[data_id] = data_dict

            # Get incoming field names
            incoming_field_names.update(data_dict.keys())

            # Determine inferred types for each field (skip 'id')
            infered_types=self._infer_pyarrow_types(data_dict)

        # Get the current schema
        current_schema = self.get_schema()
        current_names=set(current_schema.names)

        # Resolve data infered types with current schema
        infered_types=self._resolve_infered_field_types(current_schema, infered_types)

        merged_names=list(incoming_field_names.union(current_names) - set(['id']))
        update_schema = pa.schema([(field_name, infered_types[field_name]) for field_name in merged_names])

        return id_data_dict_map, update_schema

    def _infer_pyarrow_types(self, data_dict: dict):
        infered_types = {}
        for key, value in data_dict.items():
            if key != 'id':
                infered_types[key] = pa.infer_type([value])
        return infered_types

    def _validate_id(self, id: List[dict], id_column: List[int]):
        if id is None:
            raise ValueError("Each data dict must have an 'id' key.")
        if id not in id_column:
            raise ValueError(f"The id {id} is not in the main table. It may have been deleted or is incorrect.")
        return None
    
    def _resolve_infered_field_types(self, schema: pa.Schema, infered_types: dict):
        logger.info(f"Checking infered field types vs original field types")
        for field_name in schema.names:
            type=schema.field(field_name).type
            infered_types[field_name]=type

        logger.info(f"Validating field types: {infered_types}")
        return infered_types
    
    def _update_table(self, current_table: str, 
                     id_data_dict_map: dict, 
                     update_schema: pa.Schema):
        """Update a single Parquet file with new data."""

        # Add new columns to the current table
        # updated_table = self.add_null_columns_for_missing_fields( current_table, update_schema)
        logger.debug(f"Aligning table with update schema")
        updated_table = align_table(current_table, update_schema)
        logger.debug(f"Aligned table with update schema")
        # Update existing records in the current table
        current_ids_list = current_table['id'].to_pylist()
        id_to_index = {id: index for index, id in enumerate(current_ids_list)}
        # Iterate over the columns in the table
        for column_idx, column in enumerate(updated_table.itercolumns()):
            column_name = column._name
            if column_name == 'id':  # Skip 'id' column
                continue
            
            # Get current column array
            column_array = column.to_pylist()

            # Iterate over the update entries for the current column
            for id, data_dict in id_data_dict_map.items():
                
                # If the id is in the the current table 
                # and the field_nmae is in the update dict, update the column
                if id in id_to_index and column_name in data_dict:
                    
                    update_value = data_dict[column_name]

                    # If the update value is None for a column, skip it. 
                    # This can happen when a field is added to another 
                    # file but not the current one

                    if update_value is not None:  # Only update non-None values
                        # if isinstance(update_value, dict):
                        #     column_array[id_to_index[id]].update(update_value)
                        # else:
                        #     column_array[id_to_index[id]] = update_value

                        index = id_to_index[id]
                        current_value = column_array[index]

                        # Perform a deep update if it's a nested structure
                        column_array[index] = deep_update(current_value, update_value)

            # Update the column in the table
            field = updated_table.schema.field(column_name)

            # Ensure the column array is of the correct type
            column_array=pa.array(column_array, type=field.type)
            updated_table = updated_table.set_column(column_idx, field, column_array)

        return updated_table

    def _delete_ids_from_table(self, ids, table):
        """Delete IDs from a table."""
        # Get the original ids
        current_ids_list=set(table['id'].to_pylist())

        # Get the ids that are in the current table
        ids_in_table = current_ids_list & ids

        # Applying the negative id filter
        new_table = table.filter(~pc.field('id').isin(ids_in_table))
        return new_table
    
    def _update_dataset_schema(self, current_table, schema=None, field_dict=None):
        """Update the schema of a table."""
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

    def _write_tmp_files(self, tmp_dir=None):
        if tmp_dir is None:
            tmp_dir=self.tmp_dir
        shutil.rmtree(self.tmp_dir)
        os.makedirs(self.tmp_dir, exist_ok=True)

        current_filepaths=glob(os.path.join(self.dataset_dir,f'{self.dataset_name}_*.parquet'))
        for i_file, current_filepath in enumerate(current_filepaths):
            basename=os.path.basename(current_filepath)
            
            tmp_filepath = os.path.join(self.tmp_dir, basename)
            shutil.copyfile(current_filepath, tmp_filepath)

    def _restore_tmp_files(self, tmp_dir=None):
        if tmp_dir is None:
            tmp_dir=self.tmp_dir
 
        tmp_filepaths=glob(os.path.join(self.tmp_dir,f'{self.dataset_name}_*.parquet'))
        for i_file, tmp_filepath in enumerate(tmp_filepaths):
            basename=os.path.basename(tmp_filepath)
            current_filepath = os.path.join(self.dataset_dir, basename)
            shutil.copyfile(tmp_filepath, current_filepath)
            os.remove(tmp_filepath)
    
    def _validate_data(self, data):
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
        return data_list
    
    def _get_current_files(self):

        return glob(os.path.join(self.dataset_dir, f'{self.dataset_name}_*.parquet'))


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
