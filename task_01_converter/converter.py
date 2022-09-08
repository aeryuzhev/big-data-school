"""Converts csv file to parquet format or vice versa.
Creates and prints a schema of csv or parquet file."""
import sys
import pathlib
import argparse
import configparser
import logging
from logging import config as logging_config

from pyarrow import parquet, csv, lib

# Load config.
config = configparser.ConfigParser()
config.read('config.ini')

BLOCK_SIZE = config.getint('CSV Reader', 'block_size', fallback=5_242_880)
LOG_ENABLED = config.getboolean('Log', 'enabled', fallback=False)
LOG_FILE_NAME = config.get('Log', 'file_name', fallback='converter.log')
LOG_FILE_SIZE = config.getint('Log', 'file_size', fallback=52_428_800)
LOG_BACKUP_COUNT = config.getint('Log', 'backup_count', fallback=1)


def main() -> None:
    try:
        # Get arguments.
        args_dict = get_args()

        # Set up logging configuration.
        if LOG_ENABLED:
            set_up_logging()

        if args_dict['csv2parquet']:
            # Convert csv file to parquet.
            csv_to_parquet(*args_dict['csv2parquet'])
        elif args_dict['parquet2csv']:
            # Convert parquet file to csv.
            parquet_to_csv(*args_dict['parquet2csv'])
        elif args_dict['get_schema']:
            # Get and show schema of the file.
            file_schema = get_schema(args_dict['get_schema'])
            show_schema(file_schema)
    except Exception as exc:
        logging.exception(exc)
        raise
    else:
        logging.info(f'Finished {" ".join(sys.argv)}')


def csv_to_parquet(src_file: str, dst_file: str) -> None:
    """Converts csv file to parquet format."""
    src_file_schema = get_schema(src_file)
    batches = csv.open_csv(
        src_file, read_options=csv.ReadOptions(block_size=BLOCK_SIZE))

    with parquet.ParquetWriter(dst_file, src_file_schema) as pq_writer:
        for batch in batches:
            pq_writer.write_batch(batch)


def parquet_to_csv(src_file: str, dst_file: str) -> None:
    """Converts parquet file to csv format."""
    src_file_schema = get_schema(src_file)
    pq_file = parquet.ParquetFile(src_file)

    with csv.CSVWriter(dst_file, src_file_schema) as csv_writer:
        for batch in pq_file.iter_batches():
            csv_writer.write_batch(batch)


def get_schema(sch_file: str) -> lib.schema:
    """Creates schema object for csv or parquet file."""
    schema = None
    file_ext = pathlib.Path(sch_file).suffix.lower()

    if file_ext == '.csv':
        csv_file = csv.open_csv(
            sch_file, read_options=csv.ReadOptions(block_size=BLOCK_SIZE))
        schema = csv_file.schema
    elif file_ext == '.parquet':
        schema = parquet.read_schema(sch_file)
    else:
        logging.warning(
            f'Unsupported file for creating schema '
            f'with extension: {file_ext}')

    return schema


def show_schema(schema: lib.schema) -> None:
    """Shows schema."""
    if schema:
        print(schema.to_string(show_schema_metadata=False))


def get_args() -> dict:
    """Sets up the arguments settings and returns a dict with arguments.
    Prints help if no arguments are given.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()

    group.add_argument(
        '--csv2parquet', nargs=2, type=str,
        metavar=('<src-filename>', '<dst-filename>'),
        help='converts csv file to parquet format.')
    group.add_argument(
        '--parquet2csv', nargs=2, type=str,
        metavar=('<src-filename>', '<dst-filename>'),
        help='converts parquet file to csv format.')
    group.add_argument(
        '--get-schema', nargs='?', type=str, default='',
        metavar='<filename>',
        help='prints schema of the csv or parquet file.')

    # Prints help if no arguments are given.
    if len(sys.argv) <= 1:
        print(parser.print_help())

    return vars(parser.parse_args())


def set_up_logging() -> None:
    """Sets up logging configuration."""
    logging_config.dictConfig(
        {
            "version": 1,
            "root": {
                "handlers": ["file"],
                "level": "INFO"
            },
            "handlers": {
                "file": {
                    "class": "logging.handlers.RotatingFileHandler",
                    "formatter": "fmt",
                    "filename": LOG_FILE_NAME,
                    "maxBytes": LOG_FILE_SIZE,
                    "backupCount": LOG_BACKUP_COUNT
                }
            },
            "formatters": {
                "fmt": {
                    "format": "%(asctime)s | %(levelname)s | %(message)s",
                    "datefmt": "%d-%m-%Y %H:%M:%S"
                }
            }
        }
    )
    logging.info(f'Started {" ".join(sys.argv)}')


if __name__ == '__main__':
    main()
