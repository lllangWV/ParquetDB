.. _motivation:

ParquetDB Motivation
====================

The primary value of ParquetDB lies in the combination of its preprocessing steps and the robust management of the data's state over time. Essentially, ParquetDB acts as a wrapper around ``pyarrow``, incorporating crucial preprocessing functionalities. These steps are designed to save users from writing significant boilerplate code and from navigating the complexities of managing and updating immutable ``pyarrow`` tables, especially when dealing with evolving schemas and nested data structures.

Challenges with Direct File/Library Usage
------------------------------------------

For instance, if a user were to rely solely on raw ``pyarrow`` or ``pandas`` for similar tasks, they would be responsible for:

* Manually handling schema alignment between incoming data and existing datasets.
* Implementing their own row indexing mechanisms.
* Managing data updates, which becomes particularly challenging with nested structures and the immutability of Parquet files (requiring rewriting files or partitions).

Core Requirements Driving ParquetDB
-----------------------------------

When developing ParquetDB, we evaluated several existing tools, including ``Pandas``, ``Dask``, ``DuckDB``, ``PySpark``, ``Delta-rs``, and ``Polars``. Our primary use case at the time involved complex materials science data, which is often characterized by deeply nested structures and evolving schemas as research progresses. Our key requirements were:

* **Schema Evolvability**: The ability to adapt the schema over time without defining a rigid structure upfront is crucial for iterative research.
* **Handling Nested Structures**: Robust support for complex, nested data was a primary driver.
* **Performance**: The solution needed to be efficient for data storage and retrieval.
* **"Classically Serverless"**: No dependency on a network connection to a database server. This was particularly important for our work on High-Performance Computing (HPC) clusters where compute nodes often lack internet connectivity. Setting up local servers on other compute nodes felt clunky and inefficient.
* **Ability to Handle Table and Field Metadata**: Storing and managing metadata associated with tables and individual fields was also a requirement.

While ``MongoDB`` might seem like a candidate given its flexibility with schemas and nested data, it did not meet our "serverless" requirement and performance considerations for our specific analytical workloads. This led us to explore file-based solutions and build ParquetDB.

Comparison with Other Tools
---------------------------

Here's a brief comparison highlighting why other tools didn't fully meet our specific needs, leading to the development of ParquetDB:

* **Pandas**: While excellent for in-memory data manipulation and capable of reading/writing Parquet files, ``pandas`` itself doesn't inherently manage schema evolution across multiple Parquet files or simplify the complexities of updating nested data within a Parquet-based dataset. Users would need to build these management layers themselves.

* **Dask**: Similar to ``pandas``, ``Dask`` excels at parallel processing of large datasets but doesn't provide the specific schema evolution and nested data update management layer that ParquetDB offers. The onus of managing these aspects remains with the user.

* **DuckDB**: ``DuckDB`` is an impressive embedded analytical database and a strong contender for many use cases. However, at the time of our initial development and based on our exploration, its support for schema evolution, particularly with deeply nested structures, and atomic updates of individual fields within nested structures did not align with our requirements. For example, updating a single child field within a nested struct often required rewriting the entire struct.

* **Polars**: A very fast DataFrame library, but like ``pandas`` and ``Dask``, it focuses on data processing rather than the state management and schema evolution for datasets persisted as a collection of Parquet files with complex nested data. Our concerns regarding schema evolution and atomic updates of nested structures were similar to those with DuckDB.

* **Delta-rs (Delta Lake)**: Delta Lake offers ACID transactions, schema enforcement, and scalable data handling, which are very valuable. However, our primary challenge was the nuanced handling of schema *evolution* for *deeply nested* data and fine-grained updates within those nested structures, which we found ParquetDB could be tailored to address more directly for our use case.

ParquetDB's Niche
-----------------

ParquetDB was designed to fill this gap by providing a higher-level API that abstracts these complexities. The core contribution is the streamlined handling of these more intricate data management scenarios, making it easier for users to work with evolving, nested Parquet datasets. While the backend could theoretically leverage components from some of these other tools, the unique value of ParquetDB lies in its opinionated approach to preprocessing and state management that simplifies the user experience for these specific challenges.

When to Choose ParquetDB (and When Not To)
--------------------------------------------

A common question is why one might choose ParquetDB over well-known libraries like ``Pandas`` or ``Dask``.

If your data is characterized by simple, flat structures, or perhaps only shallowly nested data with a stable schema, then tools like ``DuckDB`` or managing Parquet files directly with ``pandas`` or ``pyarrow`` might indeed be sufficient and more straightforward.

However, ParquetDB becomes particularly useful when dealing with:

* Data with complex and potentially deep nesting.
* Schemas that are expected to evolve over time (e.g., adding new fields, modifying nested structures) in an iterative research or development context.
* The need for a "serverless" solution that manages collections of Parquet files as a coherent, updatable dataset.

We believe ParquetDB offers a valuable solution for users facing these specific data characteristics and workloads by simplifying the intricate aspects of managing evolving, nested data in a Parquet-based environment.