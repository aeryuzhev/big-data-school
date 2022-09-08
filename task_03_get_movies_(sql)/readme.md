# Get-movies
Get-movies is a Python module for searching movies by specified filters and printing the search result to stdout in csv-like format.

## Requirements
**Python 3.9** or higher.

This module requires the following python modules:

| Module                                                                     | Description                                                                                                                              |
|----------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| [mysql-connector-python](https://pypi.org/project/mysql-connector-python/) | MySQL driver written in Python which does not depend on MySQL C client libraries and implements the DB API v2.0 specification (PEP-249). |

This module connects to MySQL database, so you will need to [install MySQL (Linux)](https://dev.mysql.com/doc/refman/8.0/en/linux-installation.html)

## Installation

```bash
pip install -r requirements.txt
```

## Usage
```bash
get-movies.py [--N <n>] [--genres <genres>] [--year_from <year_from>] [--year_to <year_to>] [--regexp <regexp>] [--help] 
```
All arguments are optional (shows all movies if no arguments are given).

## Result
Result is sorted by following criteria:

1. rating desc
2. year desc
3. title asc

Result example:
```bash
genre,title,year,rating
Action,Tokyo Tribe,2014,5.0000
Adventure,Ice Age: The Great Egg-Scapade,2016,5.0000
Animation,Loving Vincent,2017,5.0000
Children,Ice Age: The Great Egg-Scapade,2016,5.0000
Comedy,All Yours,2016,5.0000
Crime,Loving Vincent,2017,5.0000
Documentary,Won't You Be My Neighbor?,2018,5.0000
Drama,Loving Vincent,2017,5.0000
Fantasy,L.A. Slasher,2015,5.0000
Film-Noir,Rififi (Du rififi chez les hommes),1955,4.7500
Horror,The Girl with All the Gifts,2016,5.0000
IMAX,Happy Feet Two,2011,5.0000
Musical,Holy Motors,2012,5.0000
Mystery,The Editor,2015,5.0000
Romance,All Yours,2016,5.0000
Sci-Fi,SORI: Voice from the Heart,2016,5.0000
Thriller,The Girl with All the Gifts,2016,5.0000
War,Battle For Sevastopol,2015,5.0000
Western,Trinity and Sartana Are Coming,1972,5.0000

```

## Params
| Name          | Value       | Description                                                    |
|---------------|-------------|----------------------------------------------------------------|
| `--help`      |             | shows help message                                             |
| `--N`         | <n>         | number of most rated films                                     |
| `--genre`     | <genre>     | filter by genre (can be multiple like "Comedy&#124;Animation") |
| `--year_from` | <year_from> | filter for year from                                           |
| `--year_to`   | <year_to>   | filter for year to                                             |
| `--regexp`    | <regexp>    | regular expression for searching in movie title                |

## Configs
`client/config.ini`
```ini
[Output CSV]
# Delimiter for output to stdout.
delimiter = ,
# Header names for output to stdout.
headers = genre,title,year,rating

[Log]
# Enable or disable logging.
enabled = False
# Log file name. If logging enabled, log file is saved near the 'get-movies.py'.
file_name = get-movies.log
# Max size of log file in bytes. After reaching the max size, it becomes archived.
file_size = 52428800
# Max number of log backups.
backup_count = 1

[Database]
# Database connection properties for client part.
host = localhost
port = 3306
user = user
password = password
database = movies_db
```

`server/config.ini`
```ini
[Database]
# Database connection properties for server part.
host = localhost
port = 3306
user = user
password = password
database = movies_db
```

## Examples
```bash
# Shows csv-like list of all movies.
python get-movies.py 
```
```bash
# Shows csv-like list of 3 most rated movies for every genre.
python get-movies.py --N 3
```
```bash
# Shows csv-like list of movies from the top of 20 most rated, in genre "Comedy" or "Animation", 
# in period from 2010 to 2020 and where the title contains "Abracadabra" or "Batman".
python get-movies.py --N 20 --genre "Comedy|Animation" --year_from 2000 --year_to 2010 --regex "Abracadabra|Batman"
```

## Database deployment
After installing MySQL, place the 'server/data/movies.csv' and 'server/data/ratings.csv' files to the '/var/lib/mysql-files/' directory (default MySQL directory for import in Linux). 
Enter the database connection options in the 'server/config.ini'.

Then, execute

| :warning: WARNING                                     |
|:------------------------------------------------------|
| This script will delete all data from existing tables |


```bash
./server/deploy_movies_db.sh
```
it will create database 'movie_db', create tables, load and transform data from csv files.

## Download or update csv files
Files 'movies.csv' and 'ratings.csv' added to the 'server/data' directory by default. 
But you can download or update these files by executing `server/download_csv.sh` script. 
It will download or update existing scv files 'movies.csv' and 'ratings.csv' to the 'server/data' directory.
Datasets are downloaded from [grouplens.org](https://grouplens.org/datasets/movielens/).
#### Usage
```bash 
./server/download_csv.sh --ds {small,normal,full}
```
#### Params
| Param | Value  | Description                                                                              |
|-------|--------|------------------------------------------------------------------------------------------|
|       | small  | Downloads small dataset (100,000 ratings applied to 9,000 movies by 600 users).          |
| --ds  | normal | Downloads normal dataset (25 million ratings applied to 62,000 movies by 162,000 users). |
|       | full   | Downloads full dataset (27 million ratings applied to 58,000 movies by 280,000 users).   |

## Schema of input csv files
`movies.csv`

| Field name | Type |
|------------|------|
| movieId    | int  |
| title      | str  |
| genres     | str  |

`ratings.csv`

| Field name   | Type   |
|--------------|--------|
| userId       | int    |
| movieId      | int    |
| rating       | float  |
| timestamp    | int    |
