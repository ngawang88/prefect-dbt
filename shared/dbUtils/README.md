# db-utils

`db-utils` is a Python package designed to facilitate interactions with a Redshift database. It provides utility functions for reading from and writing to the database, making it easier to manage data workflows.

## Project Structure

```
db-utils
├── src
│   ├── shared
│   │   ├── db_writer.py
│   │   └── db_reader.py
│   └── __init__.py
├── requirements.txt
├── setup.py
└── README.md
```

## Installation

To install the required dependencies, run:

```
pip install -r requirements.txt
```

## Usage

### Writing to Redshift

To write data to a Redshift database, use the `write_to_redshift` function from the `db_writer` module:

```python
from src.shared.db_writer import write_to_redshift

sql_command = "YOUR SQL COMMAND HERE"
write_to_redshift(sql_command)
```

### Reading from Redshift

To read data from a Redshift database and return it as a pandas DataFrame, use the `read_from_redshift` function from the `db_reader` module:

```python
from src.shared.db_reader import read_from_redshift

query = "YOUR SQL QUERY HERE"
df = read_from_redshift(query)
```

## Contributing

Contributions are welcome! Please feel free to submit a pull request or open an issue for any enhancements or bug fixes.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.