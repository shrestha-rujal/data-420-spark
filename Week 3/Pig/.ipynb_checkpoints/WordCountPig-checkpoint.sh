# Run pig script on Hadoop

NAME=rsh224

# Submit WordCount.pig script to pig programmatically

pig -x mapreduce -param INPUT="/data/helloworld" -param OUTPUT="/user/$NAME/word-count-pig" WordCount.pig
hdfs dfs -ls /user/$NAME/word-count-pig/
hdfs dfs -cat /user/$NAME/word-count-pig/part-r-00000

# Clean up

hdfs dfs -rm -R /user/$NAME/word-count-pig/
