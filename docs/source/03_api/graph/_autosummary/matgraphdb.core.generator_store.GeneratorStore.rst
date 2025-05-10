matgraphdb.core.generator\_store.GeneratorStore
===============================================

.. currentmodule:: matgraphdb.core.generator_store

.. autoclass:: GeneratorStore

   
   .. automethod:: __init__

   
   .. rubric:: Methods

   .. autosummary::
   
      ~GeneratorStore.__init__
      ~GeneratorStore.backup_database
      ~GeneratorStore.construct_table
      ~GeneratorStore.copy_dataset
      ~GeneratorStore.create
      ~GeneratorStore.dataset_exists
      ~GeneratorStore.delete
      ~GeneratorStore.delete_generator
      ~GeneratorStore.drop_dataset
      ~GeneratorStore.export_dataset
      ~GeneratorStore.export_partitioned_dataset
      ~GeneratorStore.get_current_files
      ~GeneratorStore.get_field_metadata
      ~GeneratorStore.get_field_names
      ~GeneratorStore.get_file_sizes
      ~GeneratorStore.get_metadata
      ~GeneratorStore.get_n_rows_per_row_group_per_file
      ~GeneratorStore.get_number_of_row_groups_per_file
      ~GeneratorStore.get_number_of_rows_per_file
      ~GeneratorStore.get_parquet_column_metadata_per_file
      ~GeneratorStore.get_parquet_file_metadata_per_file
      ~GeneratorStore.get_parquet_file_row_group_metadata_per_file
      ~GeneratorStore.get_row_group_sizes_per_file
      ~GeneratorStore.get_schema
      ~GeneratorStore.get_serialized_metadata_size_per_file
      ~GeneratorStore.import_dataset
      ~GeneratorStore.is_empty
      ~GeneratorStore.is_in
      ~GeneratorStore.list_generators
      ~GeneratorStore.load_generator
      ~GeneratorStore.load_generator_data
      ~GeneratorStore.merge_datasets
      ~GeneratorStore.normalize
      ~GeneratorStore.preprocess_table
      ~GeneratorStore.process_data_with_python_objects
      ~GeneratorStore.read
      ~GeneratorStore.rename_dataset
      ~GeneratorStore.rename_fields
      ~GeneratorStore.restore_database
      ~GeneratorStore.run_generator
      ~GeneratorStore.set_field_metadata
      ~GeneratorStore.set_metadata
      ~GeneratorStore.sort_fields
      ~GeneratorStore.store_generator
      ~GeneratorStore.summary
      ~GeneratorStore.to_nested
      ~GeneratorStore.transform
      ~GeneratorStore.update
      ~GeneratorStore.update_schema
   
   

   
   
   .. rubric:: Attributes

   .. autosummary::
   
      ~GeneratorStore.basename_template
      ~GeneratorStore.columns
      ~GeneratorStore.dataset_name
      ~GeneratorStore.db_path
      ~GeneratorStore.generator_names
      ~GeneratorStore.metadata_keys
      ~GeneratorStore.n_columns
      ~GeneratorStore.n_files
      ~GeneratorStore.n_generators
      ~GeneratorStore.n_row_groups_per_file
      ~GeneratorStore.n_rows
      ~GeneratorStore.n_rows_per_file
      ~GeneratorStore.n_rows_per_row_group_per_file
      ~GeneratorStore.required_fields
      ~GeneratorStore.serialized_metadata_size_per_file
      ~GeneratorStore.storage_path
   
   