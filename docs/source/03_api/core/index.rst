.. _core-api-index:

Core API
===================================

The Core API provides the fundamental functionality of ParquetDB, offering a robust interface for managing Parquet-based data storage. This module contains the essential classes and methods that enable database-like operations while maintaining the simplicity and portability of file-based storage.

The core components include:

- :class:`ParquetDB <parquetdb.core.parquetdb.ParquetDB>` - The main interface class that provides database-like operations over Parquet files. This class handles data storage, retrieval, querying, schema evolution, and complex data type management through an intuitive API that wraps PyArrow's functionality.

- :class:`NormalizeConfig <parquetdb.core.parquetdb.NormalizeConfig>` - A configuration dataclass that controls how data normalization operations are performed in the database. It provides settings for optimizing performance through row distribution, file structure, batch processing, and memory management. Parameters include batch sizes, read-ahead settings, threading options, and file organization constraints.

- :class:`LoadConfig <parquetdb.core.parquetdb.LoadConfig>` - A configuration dataclass that controls how ParquetDB loads data from Parquet files. It provides settings to optimize data loading performance through parameters like batch sizes, read-ahead settings, threading options, and memory management configurations.

.. toctree::
   :maxdepth: 2
   
   parquetdb
   normalize_config
   load_config
