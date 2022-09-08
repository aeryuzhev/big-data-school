"""Get-movies is a Python module for searching movies by specified filters
and printing the search result to stdout in csv-like format.
"""
import csv
import re
import sys
import argparse
import configparser
import logging
from logging import config as logging_config
from collections import defaultdict
from pathlib import Path

# Get project root path.
project_root_path = Path(__file__).parents[0]

# Load config.
config = configparser.ConfigParser()
config.read(project_root_path / 'config.ini')

MOVIES_FILE = project_root_path / config.get(
    'Files', 'movies_file', fallback=Path('data', 'movies.csv'))
RATINGS_FILE = project_root_path / config.get(
    'Files', 'ratings_file', fallback=Path('data', 'ratings.csv'))
DELIMITER_IN = config.get('Input CSV', 'delimiter', fallback=',')
ENCODING_IN = config.get('Input CSV', 'encoding', fallback='utf-8')
GENRE_SEPARATOR = config.get('Input CSV', 'genre_separator', fallback='|')
DELIMITER_OUT = config.get('Output CSV', 'delimiter', fallback=',')
ENCODING_OUT = config.get('Output CSV', 'encoding', fallback='utf-8')
HEADERS_OUT = config.get(
    'Output CSV', 'headers', fallback='genre,title,year,rating').split(',')
NUMBER_DECIMALS = config.getint('Output CSV', 'number_decimals', fallback=2)
RE_TITLE_YEAR = config.get('Regexp', 'title_year', fallback=r'(.*) \((\d{4})\)$')
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
        # Get dict with movie ids and its average ratings.
        avg_ratings = get_avg_ratings()
        # Get sorted movie list with ratings.
        movies = get_movies(avg_ratings)
        # Print csv to stdout according to filter options.
        show_result(movies, args)
    except Exception as exc:
        logging.exception(exc)
        raise
    else:
        logging.info(f'Finished {" ".join(sys.argv)}')


def get_avg_ratings() -> dict:
    """Creates and returns a dict with movies ids and its average ratings."""
    # Defaultdict(movie_id: [<sum of ratings>, <count of ratings>]).
    avg_ratings = defaultdict(lambda: [0, 0])

    with open(RATINGS_FILE, encoding=ENCODING_IN) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=DELIMITER_IN)
        next(csv_reader)

        # Row indexes: 0 - userId, 1 - movieId, 2 - rating, 3 - timestamp
        for row in csv_reader:
            # Ignore empty strings '' for all fields.
            if all(row):
                movie_id = row[1]
                rating = row[2]
                avg_ratings[movie_id][0] += float(rating)
                avg_ratings[movie_id][1] += 1

    # Return dict(movie_id: <average rating>).
    return {movie_id: sum_rates / count_rates
            for movie_id, (sum_rates, count_rates) in avg_ratings.items()}


def get_movies(avg_ratings: dict) -> list:
    """Creates and returns a list with transformed movie data."""
    with open(MOVIES_FILE, encoding=ENCODING_IN) as scv_file:
        csv_reader = csv.reader(scv_file, delimiter=DELIMITER_IN)
        next(csv_reader)

        # Assign transformed data into list.
        movies = transform_data(csv_reader, avg_ratings)

    # Sort list by rating desc, year desc, title asc.
    def sort_order(row: tuple) -> tuple:
        genre, title, year, avg_rating = row
        return genre, -avg_rating, -int(year), title

    movies = sorted(movies, key=sort_order)

    return movies


def transform_data(rows: csv.reader, avg_ratings: dict) -> list[tuple]:
    """Transforms data and returns a list with transformed data.
    Transformations:
     - splits genre
     - adds average rating
     - separates year from title
    """
    movies = []

    # Row indexes: 0 - movieId, 1 - title, 2 - genres
    for row in rows:
        genres = row[2].split(GENRE_SEPARATOR)
        avg_rating = avg_ratings.get(row[0], '')
        # Separate the year from the title.
        title = row[1]
        re_title_year = re.findall(RE_TITLE_YEAR, title) or [('', '')]
        title, year = re_title_year[0]

        # Ignore empty strings '' for all fields.
        # Ignore '(no genres listed)' for genre.
        # Not ignore zeroes in avg_rating.
        if (
            all((genres, title, year, str(avg_rating)))
            and genres[0] != '(no genres listed)'
        ):
            for genre in genres:
                avg_rating = round(avg_rating, NUMBER_DECIMALS)
                year = int(year)
                movies.append((genre, title, year, avg_rating))

    return movies


def show_result(movies: list[tuple], args: dict) -> None:
    """Prints the list to stdout (csv-like format)."""
    # Csv.writer to print into stdout.
    csv_writer = csv.writer(sys.stdout, delimiter=DELIMITER_OUT)
    csv_writer.writerow(HEADERS_OUT)
    # Genres counter for selecting top N rows in sorted list for any genre.
    genres_counter_dict = defaultdict(int)

    for row in movies:
        if get_filtering_result(row, genres_counter_dict, args):
            csv_writer.writerow(change_encoding(row))


def change_encoding(row: tuple) -> tuple:
    if ENCODING_IN != ENCODING_OUT:
        row = (str(item).encode(ENCODING_IN).decode(ENCODING_OUT)
               for item in row)
    return row


def get_filtering_result(row: tuple, genres_counter_dict: dict, args: dict) -> bool:
    """Applies filters to the row and returns boolean result of filtering."""
    filter_res = True

    genre, title, year, _ = row
    if args['year_from']:
        filter_res = (year >= args['year_from']) and filter_res
    if args['year_to']:
        filter_res = (year <= args['year_to']) and filter_res
    if args['genres']:
        filter_res = (genre in args['genres']) and filter_res
    if args['regexp']:
        filter_res = bool(re.findall(args['regexp'], title)) and filter_res
    if args['N'] and filter_res:
        genres_counter_dict[genre] += 1
        filter_res = genres_counter_dict[genre] <= args['N']

    return filter_res


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
