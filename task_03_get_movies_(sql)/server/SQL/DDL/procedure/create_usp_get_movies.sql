-- ------------------------------------------------------------------
use movies_db;
-- ------------------------------------------------------------------
drop procedure if exists usp_get_movies;
-- ------------------------------------------------------------------
delimiter $$
create procedure usp_get_movies (
    in arg_limit      int,
    in arg_genres     varchar(255),
    in arg_year_from  smallint,
    in arg_year_to    smallint,
    in arg_regexp     varchar(255)    
)
begin 
    set arg_limit = ifnull(arg_limit, 0);
    set arg_year_from = ifnull(arg_year_from, 0);
    set arg_year_to = ifnull(arg_year_to, 0); 
    set arg_genres = ifnull(arg_genres, '');
    set arg_regexp = ifnull(arg_regexp, '');

    set @query = concat(
        "select
            genre,
            title,
            `year`,
            rating
        from (
            select
                genre,
                title,
                `year`,
                rating,
                row_number() over (
                    partition by 
                        genre
                    order by
                        rating desc,
                        `year` desc,
                        title
                ) as row_num
            from 
                dst_movie
            where
                `year` >= ", arg_year_from, "
                and (", arg_year_to, " = 0 or `year` <= ", arg_year_to, ")
                and ('", arg_genres, "' = '' or genre regexp '", arg_genres, "')
                and ('", arg_regexp, "' = '' or title regexp '", arg_regexp, "')
        ) res "
    );
    
    if arg_limit <> 0 then 
        set @query = concat(@query, 'where res.row_num <= ', arg_limit);
    end if;
    
    prepare statement from @query;
    execute statement;
end $$
-- ------------------------------------------------------------------
