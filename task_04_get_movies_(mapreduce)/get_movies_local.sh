#!/bin/bash
file_name="data/movies.csv"
cat $file_name | python mapper.py | sort | python reducer.py
