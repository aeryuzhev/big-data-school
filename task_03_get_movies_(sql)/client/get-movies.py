"""Get-movies is a Python module for searching movies by specified filters
and printing the search result to stdout in csv-like format.
"""
import argparse
import csv
import sys
import configparser
import logging
from logging import config as logging_config
from pathlib import Path

from mysql.connector import connect, Error
from mysql.connector import cursor as mysql_cursor

# Get project root path.
project_root_path = Path(__file__).parents[1]

# Load client config.
config = configparser.ConfigParser()
config.read(project_root_path / 'client' / 'config.ini')

HOST = config.get('Database', 'host', fallback='localhost')
PORT = config.getint('Database', 'port', fallback=3306)
USER = config.get('Database', 'user', fallback='root')
PASSWORD = config.get('Database', 'password', fallback='')
DATABASE = config.get('Database', 'database', fallback='movies_db')
DELIMITER_OUT = config.get('Output CSV', 'delimiter', fallback=',')
HEADERS_OUT = config.get(
    'Output CSV', 'headers', fallback='genre,title,year,rating').split(',')
LOG_ENABLED = config.getboolean('Log', 'enabled', fallback=False)
LOG_FILE_NAME = config.get('Log', 'file_name', fallback='get-movies.log')
LOG_FILE_SIZE = config.getint('Log', 'file_size', fallback=52_428_800)
LOG_BACKUP_COUNT = config.getint('Log', 'backup_count', fallback=1)


def main() -> None:
    try:
        # Get arguments.
        args = get_args()
        # Set up logging configuration.
        if LOG_ENABLED:
            set_up_logging()
        # Get movies query result.
        movies_query_result = get_movies_query_result(args)
        # Print to stdout.
        show_result(movies_query_result)
    except Error as exc:
        logging.exception(exc)
        raise
    except Exception as exc:
        logging.exception(exc)
        raise
    else:
        logging.info(f'Finished {" ".join(sys.argv)}')


def get_movies_query_result(args: dict) -> list[tuple]:
    """Connects to database, executes procedure to get movies list
     and returns a cursor object.
     """
    with connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE
    ) as connection:
        with connection.cursor(buffered=True) as cursor:
            args_tuple = (
                args['N'], args['genres'], args['year_from'],
                args['year_to'], args['regexp'])

            select_movies_query = "call usp_get_movies(%s, %s, %s, %s, %s)"
            cursor.execute(select_movies_query, args_tuple)

    return cursor.fetchall()


def show_result(movies: mysql_cursor) -> None:
    """Prints query data to stdout in csv-like format."""
    csv_writer = csv.writer(sys.stdout, delimiter=DELIMITER_OUT)
    csv_writer.writerow(HEADERS_OUT)

    for row in movies:
        csv_writer.writerow(row)


def get_args() -> dict:
    """Sets up the arguments settings and returns a dict with arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--N', type=int, default=0, metavar='<n>',
        help='number of most rated films.')
    parser.add_argument(
        '--genres', type=str, default='', metavar='<genres>',
        help='filter by genre (can be multiple like "Comedy|Animation").')
    parser.add_argument(
        '--year_from', type=int, default=0, metavar='<year_from>',
        help='filter for year from.')
    parser.add_argument(
        '--year_to', type=int, default=0, metavar='<year_to>',
        help='filter for year to.')
    parser.add_argument(
        '--regexp', type=str, default='', metavar='<regexp>',
        help='regular expression for searching in title.')

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
