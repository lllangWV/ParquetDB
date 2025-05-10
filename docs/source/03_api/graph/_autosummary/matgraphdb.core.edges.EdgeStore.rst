matgraphdb.core.edges.EdgeStore
===============================

.. currentmodule:: matgraphdb.core.edges

.. autoclass:: EdgeStore

   
   .. automethod:: __init__

   
   .. rubric:: Methods

   .. autosummary::
   
      ~EdgeStore.__init__
      ~EdgeStore.backup_database
      ~EdgeStore.construct_table
      ~EdgeStore.copy_dataset
      ~EdgeStore.create
      ~EdgeStore.create_edges
      ~EdgeStore.dataset_exists
      ~EdgeStore.delete
      ~EdgeStore.delete_edges
      ~EdgeStore.drop_dataset
      ~EdgeStore.export_dataset
      ~EdgeStore.export_partitioned_dataset
      ~EdgeStore.get_current_files
      ~EdgeStore.get_field_metadata
      ~EdgeStore.get_field_names
      ~EdgeStore.get_file_sizes
      ~EdgeStore.get_metadata
      ~EdgeStore.get_n_rows_per_row_group_per_file
      ~EdgeStore.get_number_of_row_groups_per_file
      ~EdgeStore.get_number_of_rows_per_file
      ~EdgeStore.get_parquet_column_metadata_per_file
      ~EdgeStore.get_parquet_file_metadata_per_file
      ~EdgeStore.get_parquet_file_row_group_metadata_per_file
      ~EdgeStore.get_row_group_sizes_per_file
      ~EdgeStore.get_schema
      ~EdgeStore.get_serialized_metadata_size_per_file
      ~EdgeStore.import_dataset
      ~EdgeStore.is_empty
      ~EdgeStore.merge_datasets
      ~EdgeStore.normalize
      ~EdgeStore.normalize_edges
      ~EdgeStore.preprocess_table
      ~EdgeStore.process_data_with_python_objects
      ~EdgeStore.read
      ~EdgeStore.read_edges
      ~EdgeStore.rename_dataset
      ~EdgeStore.rename_fields
      ~EdgeStore.restore_database
      ~EdgeStore.set_field_metadata
      ~EdgeStore.set_metadata
      ~EdgeStore.setup
      ~EdgeStore.sort_fields
      ~EdgeStore.summary
      ~EdgeStore.to_nested
      ~EdgeStore.transform
      ~EdgeStore.update
      ~EdgeStore.update_edges
      ~EdgeStore.update_schema
      ~EdgeStore.validate_edges
   
   

   
   
   .. rubric:: Attributes

   .. autosummary::
   
      ~EdgeStore.basename_template
      ~EdgeStore.columns
      ~EdgeStore.dataset_name
      ~EdgeStore.db_path
      ~EdgeStore.edge_metadata_keys
      ~EdgeStore.edge_type
      ~EdgeStore.n_columns
      ~EdgeStore.n_edges
      ~EdgeStore.n_features
      ~EdgeStore.n_files
      ~EdgeStore.n_row_groups_per_file
      ~EdgeStore.n_rows
      ~EdgeStore.n_rows_per_file
      ~EdgeStore.n_rows_per_row_group_per_file
      ~EdgeStore.required_fields
      ~EdgeStore.serialized_metadata_size_per_file
      ~EdgeStore.storage_path
   
   