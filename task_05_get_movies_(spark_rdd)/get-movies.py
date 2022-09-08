import csv
import re
import sys
import argparse

from pyspark import SparkContext, SparkConf

MOVIES_FILE = 'data/movies.csv'
RATINGS_FILE = 'data/ratings.csv'
DELIMITER_IN = ','
DELIMITER_OUT = ','
RE_TITLE_YEAR = r'(.+)[ ]+\((\d{4})\)[ ]*$'
GENRE_SEPARATOR = '|'
NUMBER_DECIMALS = 2


def main() -> None:
    args = get_args()

    conf = (
        SparkConf()
        .setMaster('local[*]')
        .setAppName('get_movies')
    )

    sc = SparkContext(conf=conf)

    rdd_movies = sc.textFile(MOVIES_FILE)
    movies_header = rdd_movies.first()
    rdd_movies = (
        rdd_movies
        .filter(lambda row: row != movies_header)
        .map(read_csv_row)
        .map(separate_year_and_title)
        .filter(all)
        .flatMap(split_by_genre)
        .filter(lambda row: get_filtering_result(row[1], args))
    )

    rdd_ratings = sc.textFile(RATINGS_FILE)
    ratings_header = rdd_ratings.first()
    rdd_ratings = (
        rdd_ratings
        .filter(lambda line: line != ratings_header)
        .map(read_csv_row)
        .map(lambda line: (line[1], float(line[2])))
        .aggregateByKey((0, 0),
                        lambda a, b: (a[0] + b, a[1] + 1),
                        lambda a, b: (a[0] + b[0], a[1] + b[1]))
        .mapValues(lambda values: round(values[0] / values[1], NUMBER_DECIMALS))
    )

    rdd_movie_ratings = (
        rdd_movies
        .join(rdd_ratings)
        .map(lambda row: (row[1][0][0], row[1][0][1], row[1][0][2], row[1][1]))
        .sortBy(lambda row: (row[0], -row[3], -row[2], row[1]))
        .map(lambda row: (row[0], (row[1:])))
        .groupByKey()
        .map(lambda row: (row[0], list(row[1])[:args['N']]))
        .flatMap(flat_values)
        .sortByKey()
    )

    load_to_stdout(rdd_movie_ratings.collect())


def read_csv_row(row: str) -> list:
    csv_reader = csv.reader([row], delimiter=DELIMITER_IN)
    row_list = next(csv_reader)
    return row_list


def separate_year_and_title(row: list) -> tuple:
    movie_id, title, genre = row
    title = title.strip()

    re_title_year = re.findall(RE_TITLE_YEAR, title) or [('', '')]
    title, year = re_title_year[0]

    return movie_id, title, year, genre


def split_by_genre(row: tuple) -> list:
    movie_id, title, year, genres = row

    return [(movie_id, (genre, title, int(year)))
            for genre in genres.split(GENRE_SEPARATOR)]


def get_filtering_result(row: tuple, args: dict) -> bool:
    filter_res = True

    genre, title, year = row
    if genre == '(no genres listed)':
        filter_res = False
    if args['year_from']:
        filter_res = (year >= args['year_from']) and filter_res
    if args['year_to']:
        filter_res = (year <= args['year_to']) and filter_res
    if args['genres']:
        filter_res = (genre in args['genres']) and filter_res
    if args['regexp']:
        filter_res = bool(re.findall(args['regexp'], title)) and filter_res

    return filter_res


def flat_values(row: list) -> tuple:
    for value in row[1]:
        yield row[0], value


def load_to_stdout(movies: list) -> None:
    csv_writer = csv.writer(sys.stdout, delimiter=DELIMITER_OUT)

    for genre, movie_tuple in movies:
        csv_writer.writerow((genre, *movie_tuple))


def get_args() -> dict:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--N', type=int, default=None, metavar='<n>',
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


if __name__ == '__main__':
    main()
