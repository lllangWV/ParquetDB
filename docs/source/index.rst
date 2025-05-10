ParquetDB Docs!
========================================

**ParquetDB** is a Python library designed to bridge the gap between traditional file storage and fully fledged databases, all while wrapping the powerful PyArrow library to streamline data input and output. By leveraging the Parquet file format, ParquetDB provides the portability and simplicity of file-based data storage alongside advanced querying features typically found in database systems.


Because **ParquetDB** is built on top of PyArrow, it seamlessly handles data types that need special processing to be compatible with the Parquet format. This reduces manual conversion and boilerplate code, allowing developers to focus on higher-level data operations. In addition, the Parquet format's columnar storage and rich metadata make it possible to efficiently perform predicate and column pushdown, leading to faster queries by reading only the subsets of data you truly need.


With **ParquetDB**, you get:

- **Fast, Efficient Columnar Storage**: Enjoy smaller file sizes and improved read performance thanks to Parquet's column-oriented design and compression.
- **Database-Like Query Capabilities**: Take advantage of predicate and column pushdown to reduce unnecessary data scans, improving I/O performance.
- **Minimal Overhead**: Achieve quick read/write speeds without the complexity of setting up or managing a larger database system.
- **Supports Complex Data Types**: Handles nested and complex data types without serialization overhead.
- **Schema Evolution**: Supports adding new fields and updating schemas seamlessly.


+++++++++++++++
Installation
+++++++++++++++

If you're new to **ParquetDB**, you're in the right place!

.. code-block:: bash

   pip install parquetdb

+++++++++++++++
Paper
+++++++++++++++

.. grid:: 2

   .. grid-item-card:: 1st Publication
      :link-type: ref
      :class-title: pyprocar-card-title

      Using ParquetDB in your research? Please consider citing or acknowledging
      us.  We have a Arxiv Publication!

      .. image:: media/images/arxiv_publication.png
         :target: https://arxiv.org/pdf/2502.05311




+++++++++++++++
What Next?
+++++++++++++++

Now that you have successfully installed ParquetDB, here are some recommended next steps:

- **Tutorials**  
  Visit the :doc:`01_tutorials/index` section for a hands-on tutorial covering the basics of creating, reading, and querying ParquetDB files.


- **Learn the Inner Details**  
  Visit the :doc:`ParquetDB Internals <02_internal/index>` section to dive deeper into ParquetDB's internals to understand how it wraps PyArrow, processes different data types, and performs efficient read/write operations.

- **Example Gallery**  
  Visit the :doc:`Example Gallery <examples/index>` section real use cases of ParquetDB.

- **Explore PyArrow**  
  ParquetDB relies on PyArrow for powerful data type handling and filtering mechanisms. For more in-depth information on PyArrow's `Table <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`__ structure, filtering, and other features, refer to the `PyArrow Documentation <https://arrow.apache.org/docs/python/getstarted.html>`__.


Citing ParquetDB
==================

If you use ParquetDB in your work, please cite the following paper:

.. code-block:: bibtex

  @misc{lang2025parquetdblightweightpythonparquetbased,
      title={ParquetDB: A Lightweight Python Parquet-Based Database}, 
      author={Logan Lang and Eduardo Hernandez and Kamal Choudhary and Aldo H. Romero},
      year={2025},
      eprint={2502.05311},
      archivePrefix={arXiv},
      primaryClass={cs.DB},
      url={https://arxiv.org/abs/2502.05311}}



Index
==================

.. toctree::
    :maxdepth: 2
    :glob:


    examples/01_tutorials/index
    02_internal/index
    examples/index
    03_api/index
    CONTRIBUTING
