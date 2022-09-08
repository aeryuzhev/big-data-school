-- ------------------------------------------------------------------
use movies_db;
-- ------------------------------------------------------------------
drop table if exists lnd_rating;
-- ------------------------------------------------------------------
create table lnd_rating (
  id           int            not null  auto_increment,
  `user_id`    int            null,
  movie_id     int            null,
  rating       decimal(6, 4)  null,
  `timestamp`  int unsigned   null,

  primary key (id)
);
-- ------------------------------------------------------------------
