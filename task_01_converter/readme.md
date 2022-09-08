# Converter

Converter is a Python module for converting files from csv to parquet and vice versa.
Also, can create and print a schema of csv or parquet file.

## Requirements
**Python 3.6** or higher.

This module requires the following modules:

| Module                                                 | Description                                                                                                                                                                                                           |
|--------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [pyarrow](https://pypi.org/project/pyarrow/)           | This library provides a Python API for functionality provided by the Arrow C++ libraries, along with tools for Arrow integration and interoperability with pandas, NumPy, and other software in the Python ecosystem. |

## Installation

```bash
pip install -r requirements.txt
```

## Usage
```bash
python converter.py [--csv2parquet | --parquet2csv <src-filename> <dst-filename>] | [--get-schema <filename>] | [--help]
```

All arguments are optional (prints help if no arguments are given).

## Params
| Name            | Value                         | Description                         |
|-----------------|-------------------------------|-------------------------------------|
| `--help`        |                               | shows help message                  |
| `--csv2parquet` | <src-filename> <dst-filename> | converts csv file to parquet format |
| `--parquet2csv` | <src-filename> <dst-filename> | converts parquet file to csv format |
| `--get-schema`  | <filename>                    | prints schema of the file           |

## Config
`config.ini`
```ini
[Reader]
# How much bytes to process at a time from the input stream (for reducing RAM usage when reading big csv file).
block_size = 5242880

[Log]
# Enable or disable logging.
enabled = False
# Log file name. If logging enabled, log file is saved near the 'converter.py'.
file_name = converter.log
# Max size of log file in bytes. After reaching the max size, it becomes archived.
file_size = 52428800
# Max number of log backups.
backup_count = 1
```

## Examples

```bash
# Converts from csv to parquet.
python converter.py --csv2parquet <source_file> <destination_file>
```
```bash
# Converts from parquet to csv.
python converter.py --parquet2csv <source_file> <destination_file>
```
```bash
# Prints schema of a file.
python converter.py --get-schema <filename>
```
```bash
# Prints help.
python converter.py --help
```
