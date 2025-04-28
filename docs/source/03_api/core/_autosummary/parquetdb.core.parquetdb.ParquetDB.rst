parquetdb.core.parquetdb.ParquetDB
==================================

.. currentmodule:: parquetdb.core.parquetdb

.. autoclass:: ParquetDB

   
   .. automethod:: __init__

   
   .. rubric:: Methods

   .. autosummary::
   
      ~ParquetDB.__init__
      ~ParquetDB.backup_database
      ~ParquetDB.construct_table
      ~ParquetDB.copy_dataset
      ~ParquetDB.create
      ~ParquetDB.dataset_exists
      ~ParquetDB.delete
      ~ParquetDB.drop_dataset
      ~ParquetDB.export_dataset
      ~ParquetDB.export_partitioned_dataset
      ~ParquetDB.get_current_files
      ~ParquetDB.get_field_metadata
      ~ParquetDB.get_field_names
      ~ParquetDB.get_file_sizes
      ~ParquetDB.get_metadata
      ~ParquetDB.get_n_rows_per_row_group_per_file
      ~ParquetDB.get_number_of_row_groups_per_file
      ~ParquetDB.get_number_of_rows_per_file
      ~ParquetDB.get_parquet_column_metadata_per_file
      ~ParquetDB.get_parquet_file_metadata_per_file
      ~ParquetDB.get_parquet_file_row_group_metadata_per_file
      ~ParquetDB.get_row_group_sizes_per_file
      ~ParquetDB.get_schema
      ~ParquetDB.get_serialized_metadata_size_per_file
      ~ParquetDB.import_dataset
      ~ParquetDB.is_empty
      ~ParquetDB.merge_datasets
      ~ParquetDB.normalize
      ~ParquetDB.preprocess_data_without_python_objects
      ~ParquetDB.preprocess_table
      ~ParquetDB.process_data_with_python_objects
      ~ParquetDB.read
      ~ParquetDB.rename_dataset
      ~ParquetDB.rename_fields
      ~ParquetDB.restore_database
      ~ParquetDB.set_field_metadata
      ~ParquetDB.set_metadata
      ~ParquetDB.sort_fields
      ~ParquetDB.summary
      ~ParquetDB.to_nested
      ~ParquetDB.transform
      ~ParquetDB.update
      ~ParquetDB.update_schema
   
   

   
   
   .. rubric:: Attributes

   .. autosummary::
   
      ~ParquetDB.basename_template
      ~ParquetDB.columns
      ~ParquetDB.dataset_name
      ~ParquetDB.db_path
      ~ParquetDB.n_columns
      ~ParquetDB.n_files
      ~ParquetDB.n_row_groups_per_file
      ~ParquetDB.n_rows
      ~ParquetDB.n_rows_per_file
      ~ParquetDB.n_rows_per_row_group_per_file
      ~ParquetDB.serialized_metadata_size_per_file
   
   