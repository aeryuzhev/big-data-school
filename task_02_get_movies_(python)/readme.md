# Get-movies
Get-movies is a Python module for searching movies by specified filters and printing the search result to stdout in csv-like format. 

## Requirements
**Python 3.9** or higher.

This module doesn't require the installation of third-party modules.

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
Documentary,Won't You Be My Neighbor?,2018,5.0
Documentary,Blue Planet II,2017,5.0
Documentary,I Am Not Your Negro,2017,5.0
Animation,Loving Vincent,2017,5.0
Crime,Loving Vincent,2017,5.0
Drama,Loving Vincent,2017,5.0
Documentary,Tickling Giants,2017,5.0
Comedy,All Yours,2016,5.0
Drama,All Yours,2016,5.0
Romance,All Yours,2016,5.0
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

## Config
`config.ini`
```ini
[Files]
# Source for movies csv.
movies_file = data/movies.csv
# Source for ratings csv.
ratings_file = data/ratings.csv

[Input CSV]
# Delimiter for source CSVs.
delimiter = ,
# Encoding for source CSVs.
encoding = utf-8
# Separator in genres
genre_separator = |

[Output CSV]
# Delimiter for output to stdout.
delimiter = ,
# Encoding for output to stdout.
encoding = utf-8
# Header names for output to stdout.
headers = genre,title,year,rating
# Number of decimals in average rating in result.
number_decimals = 2

[Regexp]
# Regular expression for extracting title and year.
title_year = (.*) \((\d{4})\)$

[Log]
# Enable or disable logging.
enabled = False
# Log file name. If logging enabled, log file is saved near the 'get-movies.py'.
file_name = get-movies.log
# Max size of log file in bytes. After reaching the max size, it becomes archived.
file_size = 52428800
# Max number of log backups.
backup_count = 1
```

## Examples
```bash
# Shows csv-like list of all movies.
python get-movies.py 
```
```bash
# Shows csv-like list of 3 most rated movies.
python get-movies.py --N 3
```
```bash
# Shows csv-like list of movies from the top of 20 most rated, in genre "Comedy" or "Animation", 
# in period from 2010 to 2020 and where the title contains "Abracadabra" or "Batman".
python get-movies.py --N 20 --genre "Comedy|Animation" --year_from 2000 --year_to 2010 --regex "Abracadabra|Batman"
```

## Download or update csv files
Files 'movies.csv' and 'ratings.csv' added to the 'data' directory by default. 
But you can download or update these files by executing `download_csv.sh` script. 
It will download or update existing scv files 'movies.csv' and 'ratings.csv' to the 'data' directory.
Datasets are downloaded from [grouplens.org](https://grouplens.org/datasets/movielens/).
#### Usage
```bash 
./download_csv.sh --ds {small,normal,full}
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


