
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
