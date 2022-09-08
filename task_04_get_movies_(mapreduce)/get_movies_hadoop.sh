#!/bin/bash

hdfs dfs -rm -r /user/get_movies/output

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
-D mapred.reduce.tasks=1 \
-D mapred.map.tasks=1 \
-input /user/get_movies/input/movies.csv \
-output /user/get_movies/output \
-file /home/aeryuzhev/mr_test/mapper.py \
-file /home/aeryuzhev/mr_test/reducer.py \
-mapper mapper.py \
-reducer reducer.py
