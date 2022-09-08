-- ------------------------------------------------------------------
use movies_db;
-- ------------------------------------------------------------------
drop table if exists dst_movie;
-- ------------------------------------------------------------------
create table dst_movie (
    id        int            not null  auto_increment,
    movie_id  int            not null,             
    title     varchar(255)   not null,
    `year`    smallint       not null,
    genre     varchar(30)    not null,
    rating    decimal(6, 4)  not null,

    primary key (id)
);
-- ------------------------------------------------------------------

