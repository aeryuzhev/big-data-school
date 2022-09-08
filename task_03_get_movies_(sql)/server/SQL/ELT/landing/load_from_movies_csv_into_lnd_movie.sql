-- ------------------------------------------------------------------
use movies_db;
-- ------------------------------------------------------------------
truncate lnd_movie;
-- ------------------------------------------------------------------
load data 
    infile '/var/lib/mysql-files/movies.csv'
    into table lnd_movie
    fields 
        terminated by ','    
        enclosed by '"'
    lines 
        terminated by '\r\n'
    ignore 1 rows
    (movie_id, title, genres);
-- ------------------------------------------------------------------
