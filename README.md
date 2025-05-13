# ParquetDB

[Documentation][docs] | [PyPI][pypi] | [GitHub][github]


**ParquetDB** is a Python library designed to bridge the gap between traditional file storage and fully fledged databases, all while wrapping the powerful PyArrow library to streamline data input and output. By leveraging the Parquet file format, ParquetDB provides the portability and simplicity of file-based data storage alongside advanced querying features typically found in database systems.

## Table of Contents

- [ParquetDB](#parquetdb)
  - [Table of Contents](#table-of-contents)
  - [Documentation](#documentation)
  - [Features](#features)
  - [Installation](#installation)
  - [Usage](#usage)
    - [Creating a Database](#creating-a-database)
    - [Adding Data](#adding-data)
    - [Normalizing](#normalizing)
    - [Reading Data](#reading-data)
    - [Updating Data](#updating-data)
    - [Deleting Data](#deleting-data)
  - [Citing ParquetDB](#citing-parquetdb)
  - [Contributing](#contributing)
  - [License](#license)

## Documentation

Check out the [docs][docs]

## Features

- **Simple Interface**: Easy-to-use methods for creating, reading, updating, and deleting data.
- **Minimal Overhead**: Achieve quick read/write speeds without the complexity of setting up or managing a larger database system.
- **Batching**: Efficiently handle large datasets by batching operations.
- **Supports Complex Data Types**: Handles nested and complex data types.
- **Schema Evolution**: Supports adding new fields and updating schemas seamlessly.
- **Supports storing of python objects**: ParquetDB can store python objects (objects and functions) using pickle.
- **Supports np.ndarrays**: ParquetDB can store ndarrays.

## Installation

Install ParquetDB using pip:

```bash
pip install parquetdb
```

## Usage

### Creating a Database

Initialize a ParquetDB instance by specifying the path the name of the dataset

```python
from parquetdb import ParquetDB

db = ParquetDB(db_path='parquetdb')
```

### Adding Data

Add data to the database using the `create` method. Data can be a dictionary, a list of dictionaries, or a Pandas DataFrame.

```python
data = [
    {'name': 'Charlie', 'age': 28, 'occupation': 'Designer'},
    {'name': 'Diana', 'age': 32, 'occupation': 'Product Manager'}
]

db.create(data)
```

### Normalizing

Normalization is a crucial process for ensuring the optimal performance and efficient management of data. In the context of file-based databases, like the ones used in ParquetDB, normalization helps balance the distribution of data across multiple files. Without proper normalization, files can end up with uneven row counts, leading to performance bottlenecks during operations like queries, inserts, updates, or deletions.

This method does not return anything but modifies the dataset directory in place, ensuring a more consistent and efficient structure for future operations.

```python
from parquetdb import NormalizeConfig

db.normalize(
    normalize_config=NormalizeConfig(
    load_format='batches',      # Uses the batch generator to normalize
    batch_readahead=10,         # Controls the number of batches to load in memory a head of time.
    fragment_readahead=2,       # Controls the number of files to load in memory ahead of time.
    batch_size = 100000,        # Controls the batchsize when to use when normalizing. This will have impacts on amount of RAM consumed
    max_rows_per_file=500000,   # Controls the max number of rows per parquet file
    max_rows_per_group=500000)  # Controls the max number of rows per group parquet file
)
```

### Reading Data

Read data from the database using the `read` method. You can filter data by IDs, specify columns, and apply filters.

```python
# Read all data
all_employees = db.read()

# Read specific columns
names = db.read(columns=['name'])

# Read data with filters
from pyarrow import compute as pc

age_filter = pc.field('age') > 30
older_employees = db.read(filters=[age_filter])
```

### Updating Data

Update existing records in the database using the `update` method. Each record must include the `id` field.

```python
update_data = [
    {'id': 1, 'occupation': 'Senior Engineer'},
    {'id': 3, 'age': 29}
]

db.update(update_data)
```

### Deleting Data

Delete records from the database by specifying their IDs.

```python
db.delete(ids=[2, 4])
```

## Citing ParquetDB

If you use ParquetDB in your work, please cite the following paper:

```bibtex
    @misc{lang2025parquetdblightweightpythonparquetbased,
      title={ParquetDB: A Lightweight Python Parquet-Based Database}, 
      author={Logan Lang and Eduardo Hernandez and Kamal Choudhary and Aldo H. Romero},
      year={2025},
      eprint={2502.05311},
      archivePrefix={arXiv},
      primaryClass={cs.DB},
      url={https://arxiv.org/abs/2502.05311}}
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request on GitHub. More information can be found in the [CONTRIBUTING.md][contributing] file.

## License

This project is licensed under the MIT License. See the [LICENSE][license] file for details.

---


[docs]: https://parquetdb.readthedocs.io/en/latest/
[pypi]: https://pypi.org/project/parquetdb/
[github]: https://github.com/lllangWV/ParquetDB
[contributing]: https://github.com/lllangWV/ParquetDB/blob/main/CONTRIBUTING.md
[license]: https://github.com/lllangWV/ParquetDB/blob/main/LICENSE