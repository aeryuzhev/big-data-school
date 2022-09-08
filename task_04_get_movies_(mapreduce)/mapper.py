#!/usr/bin/python3

import sys
import re
import csv
import argparse
from typing import TextIO

RE_TITLE_YEAR = r'(.+)[ ]+\((\d{4})\)[ ]*$'
GENRE_SEPARATOR = '|'
CSV_DELIMITER = ','
STD_DELIMITER = '\t'


def main() -> None:
    args = get_args()
    for key, value in map(sys.stdin, args):
        print(key, value, sep=STD_DELIMITER)


def map(stdin: TextIO, args: dict) -> tuple:
    for line in csv.reader(stdin, delimiter=CSV_DELIMITER):
        _, title, genres = line
        genres_list = genres.split(GENRE_SEPARATOR)
        re_title_year = re.findall(RE_TITLE_YEAR, title.strip()) or [('', '')]
        title, year = re_title_year[0]

        for genre in genres_list:
            if all((genre, title, year)) and genre != '(no genres listed)':
                year = int(year)
                if get_filtering_result((genre, title, year), args):
                    yield genre, (title, year)


def get_filtering_result(row: tuple, args: dict) -> bool:
    filter_res = True

    genre, title, year = row
    if args['year_from']:
        filter_res = (year >= args['year_from']) and filter_res
    if args['year_to']:
        filter_res = (year <= args['year_to']) and filter_res
    if args['genres']:
        filter_res = (genre in args['genres']) and filter_res
    if args['regexp']:
        filter_res = bool(re.findall(args['regexp'], title)) and filter_res

    return filter_res


def get_args() -> dict:
    parser = argparse.ArgumentParser()
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
