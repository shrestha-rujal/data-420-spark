# List files and directories in HDFS

NAME=rujal

hdfs dfs -ls /data/  # shared data directory
hdfs dfs -ls /user/
hdfs dfs -ls /user/$NAME/  # your user directory

# Copy data already in HDFS to your user directory

hdfs dfs -cp /data/helloworld/ /user/$NAME/
hdfs dfs -ls /user/$NAME/helloworld/
hdfs dfs -cat /user/$NAME/helloworld/part-i-00000

# Copy data from HDFS

hdfs dfs -copyToLocal /user/$NAME/helloworld/part-i-00000 ~/part-i-00000
cat ~/part-i-00000

# Delete data from HDFS

hdfs dfs -rm -r /user/$NAME/helloworld/

# Copy data in and out of HDFS

mkdir -p /tmp/data/
hdfs dfs -copyToLocal /data/helloworld /tmp/data/helloworld
ls -al /tmp/data/helloworld/
hdfs dfs -copyFromLocal /tmp/data/helloworld /user/$NAME/helloworld
hdfs dfs -ls /user/$NAME/helloworld/
hdfs dfs -cat /user/$NAME/helloworld/part-i-00000

# Detailed report of data in HDFS

hdfs fsck /user/$NAME/helloworld/part-i-00000 -files -blocks -locations
