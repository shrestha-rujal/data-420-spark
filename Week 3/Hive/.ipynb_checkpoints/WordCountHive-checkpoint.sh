# Run hive query

NAME=abc123

# Configure hive metastore in ~/hive and return to ~/

mkdir -p /tmp/$NAME/hive/
cp WordCount.sql /tmp/$NAME/hive/WordCount.sql
cd /tmp/$NAME/hive/
schematool -initSchema -dbType derby  # note that this can take a while to complete but it should
ls -1

# Submit WordCount.sql script to hive to run programmatically 

hive --hivevar input="'/data/helloworld'" --hivevar output="'/user/$NAME/word-count-hive'" -f WordCount.sql
hdfs dfs -ls /user/$NAME/word-count-hive/
hdfs dfs -cat /user/$NAME/word-count-hive/000000_0

# Clean up

hdfs dfs -rm -R /user/$NAME/word-count-hive/
