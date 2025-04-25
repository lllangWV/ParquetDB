ParquetDB Internals
====================

This document describes how ParquetDB handles data internally, detailing the integration of PyArrow for in-memory processing and the data flow that standardizes, preprocesses, and writes data to disk in Parquet format.

Integration of PyArrow in ParquetDB
------------------------------------

PyArrow serves as the computational backbone of ParquetDB, providing fast in-memory processing of data stored in Parquet format while including utilities for handling complex data types, performing efficient filtering and casting operations, and parallelizing I/O tasks. It enables efficient in-memory data manipulation, which is crucial for machine learning and data-intensive workflows. Its high-performance reading and writing capabilities minimize the overhead of frequent data operations.

Since the core of PyArrow is written in C++, it allows for parallelism on threads by bypassing Python's Global Interpreter Lock (GIL), which often restricts Python's performance for parallel tasks. Additionally, PyArrow's API compatibility with tools like Apache Spark and Dask facilitates the integration of ParquetDB into existing big data pipelines.

Data Flow in ParquetDB
----------------------

ParquetDB is designed to handle various data formats and convert them into a unified format for efficient processing and storage. Whether the input data is provided as a Python list of dictionaries, a dictionary of column arrays, a pandas DataFrame, or a PyArrow Table, ParquetDB standardizes the data into a PyArrow Table before any further operations are performed. This conversion ensures consistency and leverages PyArrow's powerful data handling capabilities.

The typical data flow in ParquetDB (used in the create and update functions) is outlined below.

Input Data Formats
==================

ParquetDB accepts data in multiple formats and standardizes the input into a PyArrow Table. In our evaluation, we conducted a study of the impact of various input data formats on update performance. We focused on the following four common formats, chosen for their prevalence in Python-based data workflows and their compatibility with the underlying PyArrow backend:

- ``pylist``: Python lists
- ``pydict``: Python dictionaries
- ``pandas``: Pandas DataFrames
- ``pyarrow``: PyArrow Tables

The experiment aimed to evaluate update performance across varying dataset sizes—from small datasets with a few thousand rows to larger scales. The update times were measured for each input format, and the results are summarized in Figure :ref:`fig:data-formats`.

.. figure:: ../media/images/data_input_experiment.png
   :width: 100%
   :align: center
   :name: fig:data-formats

   **Figure 6.** Update Time vs. Number of Rows for Different Data Formats in ParquetDB. The inset displays a log plot for better visualization at smaller scales.

The performance analysis revealed that for datasets with up to several thousand rows, all formats exhibit relatively comparable update times. However, as the number of rows increases, significant differences emerge:

- **PyArrow Tables and Pandas DataFrames:**  
  These formats exhibit the best performance. Their native compatibility with PyArrow minimizes data conversion overhead, allowing update times to remain consistent even as dataset size scales. This is largely due to the fact that ParquetDB internally manages data as PyArrow Tables.

- **Python Dictionaries (pydict):**  
  Although dictionaries store homogeneous data contiguously in memory (which can be beneficial), they still require conversion to PyArrow's internal representation. This conversion introduces additional processing overhead, resulting in moderately inferior performance compared to PyArrow Tables and Pandas DataFrames.

- **Python Lists (pylist):**  
  Python lists demonstrate the poorest performance, especially as dataset size grows. Their non-contiguous memory allocation necessitates extensive type conversions, significantly reducing memory efficiency and slowing update operations.

These benchmark results indicate that for optimal update performance in ParquetDB—particularly when dealing with large datasets—using PyArrow Tables or Pandas DataFrames is highly recommended. Although Python lists and dictionaries remain viable, they incur non-trivial type conversion overhead that adversely affects scalability and efficiency.

Preprocessing Incoming Data
============================

Once the data is in the form of a PyArrow Table, ParquetDB performs several preprocessing steps to ensure that the data is correctly formatted and compatible with the Parquet format:

.. .. raw:: html

..    <video width="100%" controls>
..       <source src="_static/DataFrameFlatten.mp4" type="video/mp4">
..       Your browser does not support the video tag.
..    </video>


.. video:: ../media/videos/DataFrameFlatten.mp4
   :width: 800
   :autoplay:

The animation above demonstrates the creation of the dummy variable, the flattening of nested data structures, and the column reordering in ParquetDB. 

- **Handling Empty Structs:**  
  In Parquet, empty nested structures (such as dictionaries or structs) cannot be stored directly. To address this limitation and preserve data integrity, ParquetDB introduces a dumm variable into any empty struct fields. This ensures that all fields, even nested and potentially empty ones, contain valid data.

- **Flattening Nested Structures:**  
  ParquetDB simplifies nested data structures (e.g., structs or dictionaries with nested fields) by flattening them into a single-level table. During this process, new columns are generated for each nested field using a naming convention (e.g., ``parent.child1.child2`` for deeply nested fields). For example, a field named ``address`` with nested fields ``city`` and ``postal_code`` would produce columns named ``address.city`` and ``address.postal_code``. This flattening simplifies the schema, eases data access and analysis, and facilitates the detection of schema changes.

- **Column Reordering:**  
  Columns are alphabetically ordered by ParquetDB to simplify schema change detection and updates. New columns introduced through updates or changes are automatically placed in their correct positions, making schema differences easier to identify.

- **Schema Alignment:**  
  The incoming data is compared against the existing dataset schema. If differences are detected, ParquetDB updates the schema or casts the data to match the existing schema, depending on the operation.

- **Writing to Parquet:**  
  Finally, the preprocessed data is written to disk as a Parquet file using PyArrow's efficient file-writing utilities. At this stage, ParquetDB may also invoke the normalization process to ensure that the data is evenly distributed across files and that any schema changes, updates, or deletions are consistently applied across the dataset.

This internal data handling process ensures that all data entering ParquetDB is standardized, optimized, and ready for high-performance analytics.

