-- ------------------------------------------------------------------
use movies_db;
-- ------------------------------------------------------------------
drop table if exists lnd_movie;
-- ------------------------------------------------------------------
create table lnd_movie (
  id        int           not null  auto_increment,
  movie_id  int           null,             
  title     varchar(255)  null,  
  genres    varchar(255)  null,

  primary key (id)
);
-- ------------------------------------------------------------------
