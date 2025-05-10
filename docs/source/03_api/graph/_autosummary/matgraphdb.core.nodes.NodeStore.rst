matgraphdb.core.nodes.NodeStore
===============================

.. currentmodule:: matgraphdb.core.nodes

.. autoclass:: NodeStore

   
   .. automethod:: __init__

   
   .. rubric:: Methods

   .. autosummary::
   
      ~NodeStore.__init__
      ~NodeStore.backup_database
      ~NodeStore.construct_table
      ~NodeStore.copy_dataset
      ~NodeStore.create
      ~NodeStore.create_nodes
      ~NodeStore.dataset_exists
      ~NodeStore.delete
      ~NodeStore.delete_nodes
      ~NodeStore.drop_dataset
      ~NodeStore.export_dataset
      ~NodeStore.export_partitioned_dataset
      ~NodeStore.get_current_files
      ~NodeStore.get_field_metadata
      ~NodeStore.get_field_names
      ~NodeStore.get_file_sizes
      ~NodeStore.get_metadata
      ~NodeStore.get_n_rows_per_row_group_per_file
      ~NodeStore.get_number_of_row_groups_per_file
      ~NodeStore.get_number_of_rows_per_file
      ~NodeStore.get_parquet_column_metadata_per_file
      ~NodeStore.get_parquet_file_metadata_per_file
      ~NodeStore.get_parquet_file_row_group_metadata_per_file
      ~NodeStore.get_row_group_sizes_per_file
      ~NodeStore.get_schema
      ~NodeStore.get_serialized_metadata_size_per_file
      ~NodeStore.import_dataset
      ~NodeStore.initialize
      ~NodeStore.is_empty
      ~NodeStore.merge_datasets
      ~NodeStore.normalize
      ~NodeStore.normalize_nodes
      ~NodeStore.preprocess_table
      ~NodeStore.process_data_with_python_objects
      ~NodeStore.read
      ~NodeStore.read_nodes
      ~NodeStore.rename_dataset
      ~NodeStore.rename_fields
      ~NodeStore.restore_database
      ~NodeStore.set_field_metadata
      ~NodeStore.set_metadata
      ~NodeStore.sort_fields
      ~NodeStore.summary
      ~NodeStore.to_nested
      ~NodeStore.transform
      ~NodeStore.update
      ~NodeStore.update_nodes
      ~NodeStore.update_schema
   
   

   
   
   .. rubric:: Attributes

   .. autosummary::
   
      ~NodeStore.basename_template
      ~NodeStore.columns
      ~NodeStore.dataset_name
      ~NodeStore.db_path
      ~NodeStore.n_columns
      ~NodeStore.n_features
      ~NodeStore.n_files
      ~NodeStore.n_nodes
      ~NodeStore.n_row_groups_per_file
      ~NodeStore.n_rows
      ~NodeStore.n_rows_per_file
      ~NodeStore.n_rows_per_row_group_per_file
      ~NodeStore.name_column
      ~NodeStore.node_metadata_keys
      ~NodeStore.serialized_metadata_size_per_file
      ~NodeStore.storage_path
   
   