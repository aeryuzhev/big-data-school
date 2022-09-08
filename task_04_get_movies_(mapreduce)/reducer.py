#!/usr/bin/python3

import sys
import csv
import argparse
from itertools import groupby
from operator import itemgetter
from ast import literal_eval
from typing import TextIO

STD_DELIMITER = '\t'
DELIMITER_OUT = ','


def main() -> None:
    args = get_args()

    csv_writer = csv.writer(sys.stdout, delimiter=DELIMITER_OUT)

    for group in shuffle(sys.stdin):
        for key, values in group:
            for result_key, result_value in reduce(key, values, args):
                csv_writer.writerow((result_key, *result_value))


def shuffle(stdin: TextIO, num_reducers: int = 1) -> list:
    shuffled_list = []
    reducers_list = []
    lines_list = [line.strip().split(STD_DELIMITER) for line in stdin]

    for key, value in groupby(lines_list, itemgetter(0)):
        shuffled_list.append((key, [movie for _, movie in value]))

    shuffled_list_length = len(shuffled_list)
    group_length = shuffled_list_length // num_reducers
    if shuffled_list_length / num_reducers != group_length:
        group_length += 1

    if shuffled_list:
        reducers_list = [shuffled_list[x : x + group_length]
                         for x in range(0, shuffled_list_length, group_length)]

    return reducers_list


def reduce(key: str, values: list, args: dict) -> tuple:
    for value in values[:args['N']]:
        yield key, literal_eval(value)


def get_args() -> dict:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--N', type=int, default=None, metavar='<n>',
        help='number of most rated films.')

    return vars(parser.parse_args())


if __name__ == '__main__':
    main()
