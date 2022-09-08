import sys
import csv
import argparse

from pyspark.sql import SparkSession

MOVIES_FILE = 'data/movies.csv'
RATINGS_FILE = 'data/ratings.csv'
RE_TITLE_YEAR = r"'(.+)[ ]+\\((\\d{4})\\)[ ]*$'"
GENRE_SEPARATOR = r"'\\|'"
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
    extract_ratings(spark)
    extract_movies(spark)

    # Transform
    filtered_movies_with_ratings_df = transform_movies_and_ratings(spark, args)

    # Load
    load_to_stdout(filtered_movies_with_ratings_df.collect())

    spark.stop()


def transform_movies_and_ratings(spark, args):
    filtered_movies_with_ratings_df = spark.sql(
        f"""
        with 
        cte_aggregated_avg_ratings as (
            select 
                movieId as movie_id, 
                avg(rating) as avg_rating 
            from 
                lnd_ratings 
            group by 
                movie_id        
        ),
        -- -------------------------------
        cte_normalized_movies as (
            select
                movieId as movie_id,
                regexp_extract(title, {RE_TITLE_YEAR}, 1) as title,
                regexp_extract(title, {RE_TITLE_YEAR}, 2) as prod_year,
                explode(split(genres, {GENRE_SEPARATOR})) as genre
            from
                lnd_movies  
        ),
        -- -------------------------------
        cte_cleaned_movies as (
            select
                movie_id,
                title,
                prod_year,
                genre 
            from
                cte_normalized_movies
            where
                prod_year is not null
                and genre <> '(no genres listed)'
                and ifnull(trim(genre), '') <> ''                
        ),
        -- -------------------------------
        cte_filtered_movies as (
            select
                movie_id,
                title,
                cast(prod_year as int) as prod_year,
                genre             
            from
                cte_cleaned_movies
            where
                prod_year >= {args['year_from']}
                and ({args['year_to']} = 0 or prod_year <= {args['year_to']})
                and ('{args['genres']}' = '' or genre rlike('{args['genres']}'))
                and ('{args['regexp']}' = '' or title rlike('{args['regexp']}'))    
        ),
        -- -------------------------------
        cte_enriched_movies_with_ratings_and_row_number as (
            select
                m.genre,
                m.title,
                m.prod_year,
                r.avg_rating,
                row_number() over(
                    partition by
                        m.genre 
                    order by
                        r.avg_rating desc,
                        m.prod_year desc,
                        m.title    
                ) as row_num
            from
                cte_filtered_movies m
                join cte_aggregated_avg_ratings r on r.movie_id = m.movie_id
        ),
        -- -------------------------------
        cte_filtered_by_top_n_movies_by_genre as (
            select
                genre,
                title,
                prod_year,
                avg_rating,
                row_num
            from
                cte_enriched_movies_with_ratings_and_row_number
            where
                {args['N']} = 0 or row_num <= {args['N']}
        )
        -- -------------------------------
        select 
            genre,
            title,
            prod_year,
            round(avg_rating, {NUMBER_DECIMALS}) as avg_rating
        from 
            cte_filtered_by_top_n_movies_by_genre   
        order by
            genre,
            row_num
        """
    )

    return filtered_movies_with_ratings_df


def extract_movies(spark):
    # spark.sql('drop view if exists lnd_movies')
    spark.sql(
        f"""
        create temporary view lnd_movies (
            movieId int,
            title   string,
            genres  string
        )
        using csv
        options (
            header = true,
            sep = '{DELIMITER_IN}',
            path = '{MOVIES_FILE}'
        );    
        """
    )


def extract_ratings(spark):
    # spark.sql('drop view if exists lnd_ratings')
    spark.sql(
        f"""
        create temporary view lnd_ratings (
            userId    int,
            movieId   int,
            rating    float,
            timestamp int        
        )
        using csv
        options (
            header = true,
            delimiter = '{DELIMITER_IN}',
            path = '{RATINGS_FILE}'
        )
        """
    )


def load_to_stdout(movies_list):
    csv_writer = csv.writer(sys.stdout, delimiter=DELIMITER_OUT)

    for movie in movies_list:
        csv_writer.writerow(movie)


def get_args():
    parser = argparse.ArgumentParser()
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


if __name__ == '__main__':
    main()
