#!/bin/bash

# Get the path to the script directory.
script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# Get database connection parameters from config.ini.
source <(grep "=" "$script_dir/config.ini" | tr -d ' ')

# Deploy database objects.
mysql -h $host -u $user -p$password < "$script_dir/SQL/DDL/database/create_movies_db.sql"
mysql -h $host -u $user -p$password $database < "$script_dir/SQL/DDL/table/create_lnd_movie.sql"
mysql -h $host -u $user -p$password $database < "$script_dir/SQL/DDL/table/create_lnd_rating.sql"
mysql -h $host -u $user -p$password $database < "$script_dir/SQL/DDL/table/create_dst_movie.sql"
mysql -h $host -u $user -p$password $database < "$script_dir/SQL/DDL/procedure/create_usp_get_movies.sql"

# Load from sources to landing.
mysql -h $host -u $user -p$password $database < "$script_dir/SQL/ELT/landing/load_from_movies_csv_into_lnd_movie.sql"
mysql -h $host -u $user -p$password $database < "$script_dir/SQL/ELT/landing/load_from_ratings_csv_into_lnd_rating.sql"

# Load from landing to destination table.
mysql -h $host -u $user -p$password $database < "$script_dir/SQL/ELT/destination/load_from_landing_into_dst_movie.sql"
