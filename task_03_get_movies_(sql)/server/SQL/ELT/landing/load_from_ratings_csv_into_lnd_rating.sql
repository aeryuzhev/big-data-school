-- ------------------------------------------------------------------
use movies_db;
-- ------------------------------------------------------------------
truncate lnd_rating;
-- ------------------------------------------------------------------
load data 
    infile '/var/lib/mysql-files/ratings.csv'
    into table lnd_rating
    fields 
        terminated by ','
        enclosed by '"'
    lines 
        terminated by '\r\n'
    ignore 1 rows
    (user_id, movie_id, rating, `timestamp`);
-- ------------------------------------------------------------------    
