#!/bin/bash

# test the hadoop cluster by running wordcount

# create input files 
mkdir temp

cp 1mb.txt tmp/1mb.txt

# create input directory on HDFS
hadoop fs -mkdir -p temp

# put input files to HDFS
hdfs dfs -put ./temp/* temp

# run wordcount 
hadoop jar WordCount.jar test.WordCount temp outtemp

# print the input files
# echo -e "\ninput file1.txt:"
# hdfs dfs -cat input/file1.txt

# echo -e "\ninput file2.txt:"
# hdfs dfs -cat input/file2.txt

# print the output of wordcount
# echo -e "\nwordcount output:"
# hdfs dfs -cat output/part-r-00000

