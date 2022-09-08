import argparse
import csv
import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from pyspark.sql.types import *
from pyspark.sql.window import Window

MOVIES_FILE = 'data/movies.csv'
RATINGS_FILE = 'data/ratings.csv'
RE_TITLE_YEAR = r'(.+)[ ]+\((\d{4})\)[ ]*$'
GENRE_SEPARATOR = '\\|'
NUMBER_DECIMALS = 2
DELIMITER_IN = ','
DELIMITER_OUT = ','


def main():
    args = get_args()

    spark = (
        SparkSession.builder
        .master('local[*]')
        .appName('get-movies')
        .getOrCreate()
    )

    # Extract
    lnd_ratings_df = read_csv_to_df(spark, RATINGS_FILE, get_ratings_schema())
    lnd_movies_df = read_csv_to_df(spark, MOVIES_FILE, get_movies_schema())

    # Transform
    aggregated_avg_ratings_df = aggregate_avg_ratings(lnd_ratings_df)
    normalized_movies_df = normalize_movies(lnd_movies_df)
    cleaned_movies_df = clean_movies(normalized_movies_df)
    filtered_movies_df = filter_movies(cleaned_movies_df, args)
    enriched_movies_with_ratings_df = \
        enrich_movies_with_ratings(filtered_movies_df, aggregated_avg_ratings_df)
    filtered_top_n_movies_df = \
        filter_n_top_movies_by_genre(enriched_movies_with_ratings_df, args)

    # Load
    sorted_movies_df = format_sort_movies(filtered_top_n_movies_df)
    load_to_stdout(sorted_movies_df.collect())

    spark.stop()


def get_ratings_schema():
    ratings_schema = StructType([
        StructField('userId', IntegerType(), True),
        StructField('movieId', IntegerType(), True),
        StructField('rating', FloatType(), True),
        StructField('timestamp', IntegerType(), True)
    ])

    return ratings_schema


def get_movies_schema():
    movies_schema = StructType([
        StructField('movieId', IntegerType(), True),
        StructField('title', StringType(), True),
        StructField('genres', StringType(), True)
    ])

    return movies_schema


def read_csv_to_df(spark, path, schema):
    df = (
        spark.read
        .format('csv')
        .option('header', 'true')
        .option('delimiter', DELIMITER_IN)
        .schema(schema)
        .load(path)
    )

    return df


def aggregate_avg_ratings(ratings_df):
    ratings_df = (
        ratings_df
        .groupBy('movieId')
        .agg(sf.avg('rating').alias('avg_rating'))
    )

    return ratings_df


def normalize_movies(movies_df):
    movies_df = (
        movies_df
        .select(
            sf.col('movieId'),
            sf.regexp_extract('title', RE_TITLE_YEAR, 1).alias('title'),
            sf.regexp_extract('title', RE_TITLE_YEAR, 2).alias('year'),
            sf.explode(sf.split('genres', GENRE_SEPARATOR)).alias('genre')
        )
    )

    return movies_df


def clean_movies(movies_df):
    movies_df = (
        movies_df
        .filter(sf.col('genre') != '(no genres listed)')
        .filter(sf.col('title') != '')
        .filter(sf.col('year') != '')
    )

    return movies_df


def filter_movies(movies_df, args):
    if args['year_from']:
        movies_df = movies_df.filter(sf.col('year') >= args['year_from'])
    if args['year_to']:
        movies_df = movies_df.filter(sf.col('year') <= args['year_to'])
    if args['genres']:
        movies_df = movies_df.filter(sf.col('genre').rlike(args['genres']))
    if args['regexp']:
        movies_df = movies_df.filter(sf.col('title').rlike(args['regexp']))

    return movies_df


def enrich_movies_with_ratings(movies_df, ratings_df):
    movie_ratings_df = (
        movies_df
        .join(ratings_df, 'movieId', how='inner')
        .select(
            sf.col('genre'),
            sf.col('title'),
            sf.col('year').cast('int'),
            sf.col('avg_rating')
        )
    )

    return movie_ratings_df


def filter_n_top_movies_by_genre(movies_df, args):
    top_n_movies = args['N']

    if top_n_movies:
        window_conf = (
            Window
            .partitionBy(sf.col('genre'))
            .orderBy(
                sf.col('avg_rating').desc(),
                sf.col('year').desc(),
                sf.col('title')
            )
        )

        movies_df = (
            movies_df
            .withColumn('row_number', sf.row_number().over(window_conf))
            .filter(sf.col('row_number') <= top_n_movies)
            .drop(sf.col('row_number'))
        )

    return movies_df


def format_sort_movies(movies_df):
    movies_df = (
        movies_df
        .withColumn('avg_rating', sf.round('avg_rating', NUMBER_DECIMALS))
        .orderBy(
            sf.col('genre'),
            sf.col('avg_rating').desc(),
            sf.col('year').desc(),
            sf.col('title')
        )
    )

    return movies_df


def load_to_stdout(movies_list):
    csv_writer = csv.writer(sys.stdout, delimiter=DELIMITER_OUT)

    for movie in movies_list:
        csv_writer.writerow(movie)


def get_args() -> dict:
    parser = argparse.ArgumentParser()
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
