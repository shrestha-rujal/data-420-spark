# Python and pyspark modules required

import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession


# Required to allow the file to be submitted and run using spark-submit instead
# of using pyspark interactively

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()

# Character counts

data_path = sys.argv[1]
character_counts_path = sys.argv[2]

data = sc.textFile(data_path)

character_counts = (
    data
    # changed from splitting words to splitting all characters using list()
    .flatMap(lambda line: list(line))
    .map(lambda character: (character, 1))
    .reduceByKey(lambda x, y: x + y)
)

character_counts.saveAsTextFile(character_counts_path)
