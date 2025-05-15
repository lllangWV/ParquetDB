# Release v0.27.1 (13-05-2025)

This release focuses on streamlining documentation, enhancing type and path handling, and improving logging verbosity and metadata management. A critical bug in dataset renaming has been fixed, and dynamic node typing support has been added.

##### Bugs
- Fixed an issue in the `rename_dataset` method that previously prevented proper dataset renaming.

##### New features
- Introduced `set_node_type` for dynamic node type updates and simplified overall `node_type` management.

##### Documentation updates
- Renamed “GraphDB” to “ParquetGraphDB” across all docs and added a dedicated ParquetGraphDB guide.
- Moved tutorials to an `examples` directory and updated index.rst paths accordingly.
- Replaced all instances of “matgraphdb” with “parquetdb” and adjusted module references from `parquetdb.core` to `parquetdb.graph` in the API docs.
- Added a public section detailing ParquetDB’s motivation, advantages over pyarrow/pandas, schema-evolution and nested-data strategies, plus an internal overview of its complex-data management benefits.
- Enhanced the README with direct links to documentation, PyPI, GitHub, contributing guidelines, and license information.

##### Maintenance
- Standardized on `pathlib.Path` for file handling in ParquetDB and store constructors (now accept `str` or `Path`).
- Removed built-in metadata storage; stores now rely on user-provided metadata types and redundant initialization code/tests have been cleaned up.
- Defaulted `setup_kwargs` and `initialize_kwargs` to empty dictionaries only when not provided.
- Added type hints across ParquetDB methods for clearer interfaces.
- Tuned logging: key operations now use `debug` level to reduce noise; error logs added for unsupported or invalid load formats.
- Updated `pyproject.toml` to consolidate docs and testing dependencies under `parquetdb[docs,tests]` and added the `myst_parser` requirement.

---

# Release v0.27.0 (07-05-2025)

This release introduces a new Parquet-based graph database API complete with data validation and adjustable logging, while streamlining version management via setuptools_scm. It also upgrades testing, CI, and documentation—expanding Python support to 3.8–3.13, refining example notebooks, and improving test robustness.

##### Bugs
- Enforce the required `edge_type` input in ParquetGraphDB tests to fix edge-case failures  
- Suppress `DeprecationWarning` in ParquetGraphDB tests for cleaner output

##### New features
- Introduce core graph database components (`NodeStore`, `EdgeStore`, `ParquetGraphDB`) with DataFrame validation  
- Add a `verbose` parameter to stores and `ParquetGraphDB` for adjustable logging  
- Enhance release PR comments to include version and date for clearer CHANGELOG entries

##### Documentation updates
- Add and update ParquetGraphDB example notebooks and gallery demos  
- Remove obsolete example notebook and revise `index.rst` accordingly  
- Configure nbsphinx to skip execution in the graph generator notebook  
- Add a link in the README to CONTRIBUTING.md to guide new contributors

##### Maintenance
- Remove the obsolete `_version.py` and adopt setuptools_scm for automatic version tracking  
- Expand Python compatibility to 3.8–3.13, bump `pyarrow` to ≥17.0.0, and update GitHub Actions CI workflows  
- Limit compatibility to Python ≤3.12 in `pyproject.toml`, then broaden range and align CI accordingly  
- Refactor `PythonObjectPandasArray.__arrow_array__` for binary storage and add module logging  
- Streamline path handling in stores and remove verbose debug logs  
- Refactor and expand unit tests:  
  - Use isolated fixtures with temp directories and assert array shapes, content, and ordering  
  - Validate return types and update assertions with correct syntax  
- Update `.gitignore` to exclude example GraphDB files and data directory patterns

---

This release enhances automation across CI/CD and documentation workflows, introduces new testing utilities and PyArrow joins, and fixes temporary file naming and notebook metadata issues. It also refines configuration management, cleans up dead code, and expands test coverage for greater reliability.

##### Bugs
- Renamed temporary Parquet files from `tmp_{dataset_name}` to `tmp-{dataset_name}` to prevent race conditions in tmp-prefixed directories  
- Reset notebook execution counts and refreshed outputs for accurate metadata  
- Updated CI to install test dependencies via the `[tests]` extras and pinned Python to 3.12 for compatibility  

##### New features
- Added a GitHub Actions workflow for fully automated Python package releases and branch updates  
- Implemented scripts to generate release notes from PR commits and comments  
- Introduced a continuous pytest runner with timestamped failure logs, graceful shutdown, and iteration feedback  
- Added `join_tables` in PyArrow to perform inner, outer, and other joins with suffix handling (includes `left_outer_join` example)  
- Auto-creates a user-specific configuration file when none is found  

##### Documentation updates
- Removed the initial project note from the README and updated its link to point to `docs/index.html`  
- Created `CONTRIBUTING.md` with cloning, testing, and communication guidelines  
- Relocated Sphinx sources into `docs`, removed built docs, Makefiles, and obsolete files, and updated `.readthedocs.yaml`  
- Configured Read the Docs and added a Sphinx build workflow (Python setup, dependency install, build validation) that fails on errors  
- Enhanced notebook formatting with custom CSS and added installation instructions for `parquetdb` and `pymongo` in examples  
- Revised `paper.md` and benchmarks for clarity, updated performance comparisons and references, standardized notebook names, and corrected execution counts  

##### Maintenance
- Removed dead code: `TransactionManager` class and methods  
- Refactored `config.py` for flexible loading, added `platformdirs`, and fixed stray dependency formatting  
- Cleaned up imports, applied code formatting, and upgraded model references to `o4-mini`  
- Added `pytest` and `pytest-cov` to project dependencies and updated CI workflows accordingly  
- Clarified test data comments, re-enabled `test_update_multi_keys`, and improved temp-directory management with existence checks and debug logs  
- Replaced ad-hoc `print` statements with structured logger calls and added schema‐merge logging while removing redundant logs  
- Renamed CI jobs (`run-tests`), standardized Python to 3.10, simplified workflows by removing unnecessary steps, and added a `test-build-package` job for build/publish automation  
- Refined `_version.py` with proper formatting, comments, public API (`__all__`), and clarified type-hint imports  
- Expanded ParquetDB test suite to cover row counts, row groups, file sizes, column metadata, and dataset copying

---


___

# 0.25.1 (02-16-2025)

##### Bugs
- None identified

##### New Features
- Implemented methods to retrieve file and row group sizes in ParquetDB

##### Documentation updates
- Updated versioning information in _version.py and CHANGELOG.md for the new release

##### Maintenance
- Merged changes from the main branch of the repository

___

___

# 0.25.0 (02-15-2025)

##### Bugs
- None identified

##### New Features
- Added example notebooks for advanced 3D Alexandria and Jarvis datasets
- Introduced matplotlib utility functions and improved periodic table plotting
- Expanded test cases for new metadata retrieval functionality

##### Documentation
- Enhanced the README.md by adding a Documentation section to the table of contents and restructuring for clarity
- Created API Reference documentation for ParquetDB
- Updated documentation references to simplify class names in Core API documentation
- Prepared documentation for online hosting and integration with ReadTheDocs
- Streamlined documentation for improved readability
- Added JOSS paper metadata for ParquetDB publication
- Updated documentation configuration and static assets

##### Maintenance
- Modified dataset loading to ignore nested directories and improve filter handling
- Cleaned up and ensured consistent formatting in plotting scripts
- Revised the test runner configuration in test_parquetdb.py
- Removed the .nojekyll file from docs directory
- Merged branch 'main' from remote repository
- Updated _version.py and CHANGELOG.md for the new release

___

___

# 0.24.1 (01-26-2025)

##### Bugs
- None identified

##### New Features
- None identified

##### Documentation updates
- Updated `_version.py` and `CHANGELOG.md` for the new release

##### Maintenance
- Merged changes from the main branch of the remote repository
- Ensured the original dataset remains unchanged during transformation to a new path

___

___

# 0.24.0 (01-25-2025)

##### Bugs
- None identified

##### New Features
- Implemented a transform method in ParquetDB for flexible dataset transformations

##### Documentation updates
- Updated version information in _version.py and CHANGELOG.md due to a new release

##### Maintenance
- Merged latest changes from the main branch of the repository

___

___

# 0.23.7 (01-23-2025)

##### Bugs
- None identified

##### New features
- Enhanced dataset loading to ignore temporary files.
- Improved data type checks in loading methods.

##### Documentation updates
- Updated version number and changelog for new release.

##### Maintenance
- Refactored ParquetDB class methods for better dataset handling.
- Updated methods to accurately check for data presence.

___

___

# 0.23.6 (01-21-2025)

##### Bugs
- Fixed issue with batch deletion of columns by updating the schema before writing the dataset.

##### New features
- None identified

##### Documentation updates
- None identified

##### Maintenance
- Updated version information in `_version.py` and `CHANGELOG.md` for the new release.

___

___

# 0.23.5 (01-16-2025)

##### Bugs
- None identified

##### New features
- Enhanced the 'ParquetDB' class to improve handling of nested datasets.
- Introduced a dedicated directory for nested datasets within the database path.

##### Documentation updates
- Updated '_version.py' and 'CHANGELOG.md' to reflect the new release.

##### Maintenance
- Merged latest changes from the main branch of the remote repository.

___

___

# 0.23.4 (01-09-2025)

##### Bugs
- None identified

##### New features
- Enhanced the `PythonObjectPandasArray` class with a new `__setitem__` method for improved array manipulation.

##### Documentation updates
- Updated `_version.py` and `CHANGELOG.md` to reflect the new release.

##### Maintenance
- Merged updates from the main branch of the repository.

___

___

# 0.23.3 (01-09-2025)

##### Bugs
- None identified

##### New Features
- Enhanced ParquetDB functionality by adding new dependencies: `dask` and `distributed`.
- Improved equality check in `PythonObjectArrowScalar` class for better accuracy.
  
##### Documentation updates
- Updated `CHANGELOG.md` to reflect the new release.

##### Maintenance
- Refactored `pyarrow_utils.py` for better readability and organization.
- Revised test cases to manage `None` values in structure fields.
- Updated `pyproject.toml` to include new dependencies.

___

___

# 0.23.2 (01-08-2025)

##### Bugs
- None identified

##### New features
- Enhanced the ParquetDB class to more robustly check for empty datasets and normalize incoming table schemas.

##### Documentation updates
- Updated the CHANGELOG.md and _version.py to reflect the new release.

##### Maintenance
- Merged the latest changes from the main branch of the repository.

___

___

# 0.23.1 (01-08-2025)

##### Bugs
- None identified

##### New features
- Enhanced the `parallel_apply` function in `mp_utils.py` to allow the specification of a `processes` parameter, improving flexibility in multiprocessing for large datasets.

##### Documentation updates
- Updated `_version.py` and `CHANGELOG.md` to reflect the latest release.

##### Maintenance
- Merged changes from the main branch of the repository on GitHub.

___

___

# 0.23.0 (01-08-2025)

##### Bugs
- None identified

##### New Features
- None identified

##### Documentation
- Updated `_version.py` and `CHANGELOG.md` to reflect the new release.

##### Maintenance
- Adjusted unit tests for compatibility with new settings and functionality.
- Merged changes from the main branch of the remote repository.

___

___

# 0.22.0 (01-07-2025)

##### Bugs
- None identified

##### New Features
- None identified

##### Documentation updates
- Updated `_version.py` and `CHANGELOG.md` to reflect the new release

##### Maintenance
- Ensured consistent formatting and spacing throughout the codebase for improved readability
- Merged updates from the main branch of the repository

___

___

# 0.21.0 (01-06-2025)

##### Bugs
- None identified

##### New Features
- Enhanced ParquetDB to support Python object serialization.
- Added new utility functions for multiprocessing and plotting.

##### Documentation updates
- Updated `_version.py` and `CHANGELOG.md` for new release.

##### Maintenance
- Removed redundant utility files to improve maintainability.
- Updated unit tests to align with changes in `rename_dataset` functionality.

___

___

# 0.20.0 (01-06-2025)

##### Bugs
- None identified

##### New Features
- None identified

##### Documentation updates
- Updated `_version.py` and `CHANGELOG.md` to reflect the new release

##### Maintenance
- None identified

___

___

# 0.19.0 (01-03-2025)

##### Bugs
- None identified

##### New features
- Enhanced ParquetDB initialization to accept initial fields for schema definition.
- Refactored the `get_field_metadata` method to support retrieval of metadata for multiple field names.

##### Documentation updates
- Updated version information in `_version.py` and `CHANGELOG.md` following the new release.

##### Maintenance
- Merged changes from the main branch of the repository.

___

___

# 0.18.0 (01-03-2025)

##### Bugs
- None identified

##### New features
- Enhanced metadata handling for ParquetDB

##### Documentation updates
- Updated `_version.py` and `CHANGELOG.md` in preparation for the new release

##### Maintenance
- Merged updates from the main branch of the repository

___

___

# 0.17.0 (01-03-2025)

##### Bugs
- None identified

##### New Features
- None identified

##### Documentation
- Updated `_version.py` and `CHANGELOG.md` for new release.

##### Maintenance
- Merged changes from the main branch of the repository.

___

___

# 0.16.0 (01-02-2025)

##### Bugs
- None identified

##### New Features
- Enhanced unit tests to validate schema updates and ensure correct handling of metadata updates with the new parameter.

##### Documentation updates
- Updated `_version.py` and `CHANGELOG.md` for the new release.

##### Maintenance
- Merged changes from the main branch of the repository.

___

___

# 0.15.0 (01-02-2025)

##### Bugs
- None identified

##### New features
- Modified unit tests to validate new metadata setting behaviors, ensuring proper functionality for both update and replace actions.

##### Documentation updates
- Updated `_version.py` and `CHANGELOG.md` for the new release.

##### Maintenance
- Merged changes from the main branch of the repository.

___

___

# 0.14.0 (01-01-2025)

##### Bugs
- None identified

##### New Features
- None identified

##### Documentation updates
- Updated the CHANGELOG.md to reflect the new release.

##### Maintenance
- Refactored the ParquetDB preprocessing for improved code structure.
- Removed outdated TODO comments from `parquetdb.py` to streamline the codebase.
- Refactored update handling in ParquetDB. 
- Updated `_version.py` to align with the new release.

___

___

# 0.13.0 (01-01-2025)

##### Bugs
- None identified

##### New Features
- Enhanced the `drop` method to recreate the dataset directory and initialize an empty table when dropping an existing dataset.

##### Documentation updates
- Updated `_version.py` and `CHANGELOG.md` for the new release.

##### Maintenance
- Merged updates from the main branch of the repository.

___

___

# 0.12.0 (01-01-2025)

##### Bugs
- None identified

##### New Features
- Developed early exit functionality for handling empty datasets in ParquetDB operations.
- Enhanced table handling and schema management features in ParquetDB.

##### Documentation updates
- Updated version information and CHANGELOG for the new release.

##### Maintenance
- Merged changes from the main branch of the remote repository.

___

___

# 0.11.0 (12-30-2024)

##### Bugs
- None identified

##### New Features
- None identified

##### Documentation updates
- Updated `_version.py` and `CHANGELOG.md` for the new release

##### Maintenance
- Refactored schema merging and improved metadata handling in ParquetDB
- Merged changes from the main remote repository

___

___

# 0.10.0 (12-30-2024)

##### Bugs
- None identified

##### New Features
- Enhanced metadata handling in ParquetDB

##### Documentation updates
- Updated version information in `_version.py` and the `CHANGELOG.md` for the new release

##### Maintenance
- Improved logging functionality in ParquetDB

___

___

# 0.9.0 (12-18-2024)

##### Bugs
- None identified

##### New features
- Enhanced the update functionality to support customizable update keys in ParquetDB.

##### Documentation updates
- Updated `_version.py` and `CHANGELOG.md` to reflect the new release.

##### Maintenance
- Refactored table update logic and improved schema alignment in ParquetDB.
- Merged updates from the main branch.

___

___

# 0.8.0 (12-18-2024)

##### Bugs
- None identified

##### New features
- Refactored handling of nested dataset directories in ParquetDB.
- Added unit tests for rebuilding nested structures.

##### Documentation updates
- Updated version information in _version.py and CHANGELOG.md for the new release.

##### Maintenance
- Merged updates from the main branch of the upstream repository.

___

___

# 0.7.0 (12-18-2024)

##### Bugs
- None identified

##### New features
- None identified

##### Documentation updates
- Updated _version.py and CHANGELOG.md for the new release

##### Maintenance
- Refactored ParquetDB to ensure consistent use of db_path for directory handling
- Merged latest changes from the main branch of the repository

___

___

# 0.6.0 (12-15-2024)

##### Bugs
- None identified

##### New features
- Enhanced ParquetDB with improved logging and support for shape tensors.
- Implemented `sort_fields` method in ParquetDB with unit tests.
- Implemented `rename_fields` method in ParquetDB with unit tests.
- Added a method to set field metadata.

##### Documentation updates
- Updated `_version.py` and `CHANGELOG.md` for new release.

##### Maintenance
- Removed outdated TODO comments in `parquetdb.py`.
- Refactored ParquetDB initialization and directory handling.

___

___

# 0.5.14 (12-12-2024)

##### Bugs
- Improved handling for nested dictionaries within lists to prevent errors with empty nested lists containing structs.

##### New Features
- Introduced keyword arguments to enhance control over preprocessing of incoming data, allowing automatic conversion of lists of floats or ndarray-like structures to fixed shape tensors.

##### Documentation updates
- Updated `_version.py` and `CHANGELOG.md` in preparation for a new release.

##### Maintenance
- Unified schema handling in `pa.unify_schema` to accommodate special cases.

___

___

# 0.5.13 (12-02-2024)

##### Bugs
- None identified

##### New Features
- Added additional authors to `pyproject.toml`
- Introduced new plotting scripts for benchmarks
- Added utility functions for increased functionality
- Included example for importing the Jarvis Alexandria 2D dataset
- Added new example for importing the Jarvis DFT 3D dataset
- Improved normalization process in benchmarks

##### Documentation updates
- Updated docstrings for clarity
- Revised the first example to enhance understanding
- Updated `_version.py` and `CHANGELOG.md` for the new release

##### Maintenance
- Merged changes from the main branch of the ParquetDB repository
- Removed unnecessary logging level modifications
- Added new examples to demonstrate functionality

___

___

# 0.5.12 (10-25-2024)

##### Bugs
- Removed unnecessary deletion that did not affect outer scope variable
- Removed `ParquetDb_manager` due to an issue

##### New features
- None identified

##### Documentation updates
- Updated `_version.py` and `CHANGELOG.md` for the new release

##### Maintenance
- Merged updates from the main branch of the repository

___

___

# 0.5.11 (10-25-2024)

##### Bugs
- None identified

##### New Features
- Introduced data classes for handling normalization and loading configurations

##### Documentation updates
- Updated `_version.py` and `CHANGELOG.md` for the latest release

##### Maintenance
- Reformatted code
- Removed the `ParquetDBManager` class
- Merged updates from the main branch of repository `https://github.com/lllangWV/ParquetDB`

___

___

# 0.5.10 (10-25-2024)

##### Bugs
- Fixed bug where some `FixedListArrays` were null.
- Resolved issue where some rows were null in the method that enforces numeric and boolean list types.

##### New Features
- Introduced a new method for preprocessing incoming tables.
- Modified create and update methods to apply `table_column_callbacks`, allowing users to reconstruct ndarrays more easily.

##### Documentation updates
- Updated `_version.py` and `CHANGELOG.md` for the new release.
- Enhanced comments for better code readability.

##### Maintenance
- Updated tests to ensure functionality.
- Moved data generation logic to `general_utils`.
- Cleaned up the codebase for improved quality.

___

___

# 0.5.1 (10-25-2024)

##### Bugs
- Fixed a bug related to `FixedListArrays` when they were null.
- Resolved issues with processing rows that contained null values.

##### New features
- Introduced a new method for preprocessing the incoming table.
- Added a test for handling nested data across create, read, and update operations.
- Modified create and update functionalities to apply column callbacks for dynamic modifications.

##### Documentation updates
- Enhanced code readability with additional comments.

##### Maintenance
- Cleaned up code for better organization.
- Updated the method to restrict list types to numeric and Boolean only.
- Moved data generation code to `general_utils`.
- Updated `_version.py` and `CHANGELOG.md` for the new release.

___

___

# 0.4.1 (10-23-2024)

##### Bugs
- Fixed an issue with normalization when handling batch updates.
- Updated the method to correctly handle updates to list fields.

##### New Features
- Reworked the modification process to keep main files untouched, generating and renaming new files as needed.

##### Documentation
- Enhanced documentation for the project.

##### Maintenance
- Updated `_version.py` and `CHANGELOG.md` for new releases.
- Updated example files.
- Updated `config.yml`.
- Updated `.gitignore`.
- Merged the latest updates from the main branch.

___

___

# 0.4.0 (10-23-2024)

##### Bugs
- Fixed bug in normalization for batch updates.

##### New features
- Reworked modification handling to preserve main files and create new versions.
- Improved update method to support list field updates.

##### Documentation updates
- Updated documentation to reflect recent changes.

##### Maintenance
- Updated configuration file (`config.yml`).
- Updated `.gitignore` file.
- Updated `_version.py` and `CHANGELOG.md` for new release.
- Merged latest changes from the main branch of the repository.

___

___

# 0.3.1 (10-16-2024)

##### Bugs
- Fixed bug in batch updates resulting in incorrect chunked arrays from columns. Updated record batches to ensure casting to the incoming schema is applied correctly.

##### New Features
- Introduced capability to delete columns.
- Added new data generation methods.
- Implemented new benchmarks for performance evaluation.
- Added utility functions for matplotlib.
- Included benchmark scripts for SQLite, MongoDB, and ParquetDB.
- Enabled rebuilding of nested tables from a flattened structure in the read method.

##### Documentation Updates
- Updated the README to include a section for benchmark overview.
- Improved HTML for embedding PDFs in the README.
- Updated CHANGELOG.md and _version.py for the new release.

##### Maintenance
- Corrected default `normalize_kwargs`.
- Optimized the update table method for a 5x speed increase.
- Moved default normalization parameters to config.yml.
- Renamed directory to benchmarks and changed PDF files to PNG format.
- Updated development dependencies.

___

___

# 0.3.0 (10-16-2024)

##### Bugs
- Fixed bug in batch updates where generated column data did not produce chunked arrays. Ensured record batches are cast to the incoming schema properly.

##### New Features
- Introduced new benchmarks for performance evaluation.
- Added new data generation methods.
- Implemented column deletion functionality.
- Enhanced create and update methods to efficiently handle various input types (pylists, pydicts, pd.DataFrame, and pa.lib.Table).
- Added an option to rebuild nested tables from a flattened structure in the read method.
- Developed benchmark scripts for databases including SQLite, MongoDB, and ParquetDB.
- Added matplotlib utilities for visualization.

##### Documentation
- Updated README.md to include a benchmark overview and embedded PDF section.
- Revised dev dependencies information.

##### Maintenance
- Improved update table method, enhancing performance by five times.
- Reorganized the directory structure for benchmarks.
- Moved default normalization parameters to config.yml.
- Updated _version.py and CHANGELOG.md for the latest release.

___

___

# 0.2.7 (10-11-2024)

##### Bugs
- None identified

##### New features
- None identified

##### Documentation updates
- Updated `_version.py` and `CHANGELOG.md` for the new release

##### Maintenance
- Improved workflow scripts 
- Merged latest changes from the main branch of the repository

___

___

# 0.2.7 (10-11-2024)

##### Bugs
- None identified

##### New features
- None identified

##### Documentation updates
- Updated the workflow script

##### Maintenance
- Updated `_version.py` and `CHANGELOG.md` for the new release

___

___

# 0.2.6 (10-11-2024)

##### Bugs
- None identified

##### New Features
- Enhanced the logging mechanism in tests with separate loggers.
- Improved methods for creating, updating, and deleting schemas to support batch operations.

##### Documentation updates
- Updated example for clarity.

##### Maintenance
- Merged changes from the main branch of the repository.
- Removed unnecessary development scripts.
- Deleted obsolete file.
- Excluded `dev_scripts` from `.gitignore`.
- Updated `_version.py` and `CHANGELOG.md` for the new release.

___

___

# 0.2.5 (10-11-2024)

##### Bugs
- None identified

##### New features
- Introduced a new storage method that flattens all nested structures into a single table and sorts columns alphabetically, enhancing performance.

##### Documentation updates
- Updated `_version.py` and `CHANGELOG.md` to reflect the new release.

##### Maintenance
- Improved `.gitignore` to exclude `dev_scripts`.
- Refined tests.
- Revised example script for clarity.

___

___

# 0.2.4 (10-11-2024)

##### Bugs
- None identified

##### New Features
- Added `__version__` import to the Parquet module

##### Documentation
- Updated `_version.py` and `CHANGELOG.md` for the new release

##### Maintenance
- Merged changes from the main branch of the repository

___

___

# 0.2.3 (10-11-2024)

##### Bugs
- Fixed a bug in external_utils that prevented automatic creation of source and destination directories.

##### New features
- Added a development script to handle ordering of nested fields.
- Updated dependencies to include 'requests'.

##### Documentation updates
- Updated _version.py and CHANGELOG.md for the new release.

##### Maintenance
- Improved the workflow script to include test building of the package before pushing the version and changelog.
- Updated the workflow script for better branch management after pushing changes.

___

___

# 0.2.2 (10-10-2024)

##### Bugs
- Fixed a bug related to the old method of aligning the table with the new schema, resulting in improved performance.

##### New features
- Updated example functionality to enhance usability.

##### Documentation updates
- Updated _version.py and CHANGELOG.md for the new release.

##### Maintenance
- Merged changes from the main branch to ensure project stays up-to-date.

___

___

# 0.2.1 (10-10-2024)

##### Bugs
- Fixed issue with database row order inconsistency in tests.
- Corrected incorrect input provided during schema alignment in the create statement.

##### New Features
- Introduced a new utility function for table manipulation and empty table generation.
- Added methods for merging schemas with built-in functions.

##### Documentation updates
- Enhanced documentation with detailed docstrings for several functions.
- Updated the README with relevant information.

##### Maintenance
- Refactored multiple methods for optimization, including `create`, `update`, and `delete`.
- Improved developer scripts and added new scripts for schema merging.
- Updated `.gitignore` and revision files like `_version.py` and `CHANGELOG.md`.

___

___

# 0.1.2 (10-08-2024)

##### Bugs
- None identified

##### New Features
- Introduced a new example demonstrating the use of `ParquetDatasetDB` with 4 million structures and highlighted some reading capabilities.
- Added an option to create normalization methods that optimize performance by ensuring a consistent number of rows in dataset files.
- Implemented a script for running all tests in the test directory.

##### Documentation updates
- Updated `_version.py` and `CHANGELOG.md` for the new release.
- Consolidated logging and configuration management into a single config object.

##### Maintenance
- Added BeautifulSoup as a dependency for examples.
- Moved old examples to the `dev_scripts/examples` directory.
- Rearranged and improved the structure of `dev_scripts`.
- Updated changelog script and configuration for the timing logger.
- Merged updates from the main branch of the repository.

___

___

# 1.0.0 (10-08-2024)

##### Bugs
- None identified

##### New Features
- Introduced `ParquetDB`, the core class, with `ParquetDBManager` to facilitate management of multiple independent datasets.
- Added `beautifulsoup` as a new dependency for example usage.
- Implemented an example demonstrating the usage of `ParquetDatasetDB` to write 4 million structures and associated read capabilities.
- Added the ability to create normalization methods that optimize performance by ensuring consistent row counts in dataset directories.
- Enhanced the `merge_schema` function with `timeit` to measure performance during execution.

##### Documentation updates
- Updated the changelog script and revised `_version.py` and `CHANGELOG.md` for the new release.

##### Maintenance
- Added todos for future enhancement, specifically to remove raw table read and write operations for batch compatibility.
- Moved outdated examples to the `dev_scripts/examples` directory.
- Adjusted default values for normalization.
- Consolidated `logging_config` and `config` into a unified base object `LoggingConfig`.
- Added a script to run tests in the test directory.
- Rearranged and organized the `dev_scripts` directory.
- Merged changes from the main branch of the remote repository.

___

___

# 0.0.8 (10-08-2024)

##### Bugs
- Made changes so read will no always return an empty table or batch generator if the filtering or column selection fails
- Removed print statement

##### New Features
- Added config class. Now users can change configs by importing parquetdb; parquetdb.config.root_dir='path/to/dir' or change the logging parquetdb.logging_config.loggers.parquetdb.level='Debug'; parquetdb.logging_config.apply()
- New dev scripts

##### Documentation updates
- Updated _version.py and CHANGELOG.md due to new release

##### Maintenance
- Removed logging from tests
- Merge branch 'main' of https://github.com/lllangWV/ParquetDB into main

___

___

# 0.0.7 (10-07-2024)

##### Bugs
- Improved support for nested struct types to prevent issues with empty dictionaries.
- Enhanced `merge_tables` function and added more utility functions for manipulating `pa.struct` types.

##### New Features
- [No changes]

##### Documentation updates
- Updated README.md for clarity.
- Changed `table_name` to `dataset_name` in the README.md.
- Made deployment workflow agnostic to repository and package name.

##### Maintenance
- Updated `env_dev.yml` and `env.yml`.
- Updated package directory structure by moving `parquetdb` and `parquet_datasetdb` to `core` and creating a `utils` folder with `general_utils`.
- Updated `_version.py` and `CHANGELOG.md` due to new release.

___

___

# 0.0.6 (10-03-2024)

##### Bugs
- Bug fix: forgot to pass GitHub token and repo_name as env var
- Bug fix in workflow: got the wrong git commits

##### New Features
- None identified

##### Documentation updates
- Updated _version.py and CHANGELOG.md due to new release (noted twice)

##### Maintenance
- Merge branch 'main' of https://github.com/lllangWV/ParquetDB into main (noted twice)

___

___

# 0.0.2 (10-03-2024)

##### Bugs
- None identified
##### New features
- None identified
##### Documentation updates
- None identified
##### Maintenance
- No changes

___
