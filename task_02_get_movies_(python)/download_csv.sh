#!/bin/bash

help="Usage: download_csv.sh --ds {small,normal,full}"

if [[ "$1" = "--ds" ]]
then
  case "$2" in
    "small")
      filename="ml-latest-small.zip";;
    "normal")
      filename="ml-25m.zip";;
    "full")
      filename="ml-latest.zip";;
    *)
      echo "Incorrect parameter value: $2"
      echo "$help"
      exit 1
  esac
else
  echo "Incorrect parameter: $1"
  echo "$help"
  exit 1
fi

url="https://files.grouplens.org/datasets/movielens/$filename"
wget $url
unzip -jo $filename "*/movies.csv" "*/ratings.csv" -d "./data"
rm $filename
