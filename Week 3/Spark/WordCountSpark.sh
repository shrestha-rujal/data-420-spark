# Submit WordCount.py script to spark to run programmatically 

NAME=abc123

submit_pyspark_script "CharacterCount.py hdfs:///data/helloworld hdfs:///user/$NAME/character-count-spark"
hdfs dfs -ls /user/$NAME/character-count-spark/
hdfs dfs -cat /user/$NAME/character-count-spark/*

# Clean up

hdfs dfs -rm -R /user/$NAME/word-count-spark/
