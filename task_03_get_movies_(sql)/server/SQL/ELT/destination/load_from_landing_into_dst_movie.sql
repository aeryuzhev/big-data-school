-- ------------------------------------------------------------------
use movies_db;
-- ------------------------------------------------------------------
truncate dst_movie;
-- ------------------------------------------------------------------
set @re_year = ' \\([0-9]{4}\\)[ ]*$';
set @re_rem_brackets = '[ ()]';
set @genres_delimiter = '|';
-- ------------------------------------------------------------------
insert into dst_movie (movie_id, title, `year`, genre, rating)
with 
cte_aggregated_avg_ratings as (
    select
        r.movie_id,
        avg(r.rating) as avg_rating
    from
        lnd_rating r   
    group by 
        r.movie_id
),
-- -------------------------------
cte_enriched_movies_with_avg_rating as (
    select
        m.movie_id,
        m.title,
        m.genres,
        r.avg_rating
    from
        lnd_movie m
        join cte_aggregated_avg_ratings r on r.movie_id = m.movie_id
),
-- -------------------------------
cte_normalized_movies_title_year as (
    select
        movie_id,
        regexp_replace(
            regexp_substr(title, @re_year), 
            @re_rem_brackets, ''
        ) as prod_year,
        regexp_replace(title, @re_year, '') as title,
        genres,
        avg_rating
    from
        cte_enriched_movies_with_avg_rating
),
-- --------------------------------
cte_normalized_movies_genres as (
    select
        m.movie_id,
        trim(m.title) as title,
        cast(m.prod_year as unsigned) as prod_year,
        trim(j.genre) as genre, 
        m.avg_rating
    from
        cte_normalized_movies_title_year m
        join json_table (
            replace(json_array(m.genres), @genres_delimiter, '","'), 
            '$[*]' columns (genre varchar(30) path '$')
        ) j  
)
-- --------------------------------
select
    movie_id,
    title,
    prod_year,
    genre, 
    avg_rating
from 
    cte_normalized_movies_genres
where
    prod_year is not null
    and genre <> '(no genres listed)'
    and ifnull(trim(genre), '') <> '';
-- ------------------------------------------------------------------    

