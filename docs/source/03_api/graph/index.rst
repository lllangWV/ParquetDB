.. _core-api-index:

Core API
===================================

The Core API provides the fundamental functionality of MatGraphDB, offering a robust interface for managing a graph database. This module contains the essential classes and methods that enable database-like operations 
The core components include:

- :class:`EdgeStore <matgraphdb.core.edges.EdgeStore>` - The main interface class that provides database-like operations over Parquet files. This class handles data storage, retrieval, querying, schema evolution, and complex data type management through an intuitive API that wraps PyArrow's functionality.

- :func:`edge_generator <matgraphdb.core.edges.edge_generator>` - A decorator that validates the input arguments of a function and converts them into a dataframe.

- :class:`GeneratorStore <matgraphdb.core.generator_store.GeneratorStore>` - A store for managing generator functions in a graph database. This class handles serialization, storage, and loading of functions that generate edges between nodes.

- :class:`NodeStore <matgraphdb.core.nodes.NodeStore>` - A store for managing node features in a graph database. This class handles data storage, retrieval, querying, schema evolution, and complex data type management through an intuitive API that wraps PyArrow's functionality.

- :func:`node_generator <matgraphdb.core.nodes.node_generator>` - A decorator that validates the input arguments of a function and converts them into a dataframe.

- :class:`GraphDB <matgraphdb.core.graph_db.GraphDB>` - A manager for a graph storing multiple node types and edge types. Each node type and edge type is backed by a separate ParquetDB instance (wrapped by NodeStore or EdgeStore).

.. toctree::
   :maxdepth: 2
   
   node_store
   node_generator
   edge_store
   edge_generator
   generator_store
   graphdb
