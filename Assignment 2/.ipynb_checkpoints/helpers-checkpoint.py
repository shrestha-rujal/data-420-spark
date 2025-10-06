from pyspark.sql import functions as F
from pyspark.sql.types import *
import getpass
import pandas
import pyspark
import random
import re

# better to use environment variables for token keys, and directory paths, but hardcoded for the purposes
# of this assignment, will be refactored later if sufficient time is left for refactoring
DIRECTORY_PATH = "wasbs://campus-data@madsstorage002.blob.core.windows.net/msd"

# map data types defined in attribute files to spark data types
TYPE_MAPPER = {
    "real": FloatType(),
    "numeric": FloatType(),
    "integer": IntegerType(),
    "string": StringType()
}

# single function to automate everything. This function loads the attributes, extracts the schema,
# loads the associated feature and renames the columns
# prepares and returns the dataset with formatted column names ready for use
def load_feature(spark, feature_name):
    attributes = spark.read.csv(f'{DIRECTORY_PATH}/audio/attributes/{feature_name}.attributes.csv')   

    attributes_list = [(row._c0.strip(), row._c1.strip()) for row in attributes.collect()]

    # define schema using the attributes file
    schema = StructType([
        StructField(name, TYPE_MAPPER.get(dtype.lower(), StringType()), True)
        for name, dtype in attributes_list
    ])

    # load features using schema
    features = spark.read.csv(
        f'{DIRECTORY_PATH}/audio/features/{feature_name}.csv',
        schema=schema,
        header=False
    )

    # make primary identifier column consistent, so it is easier to join later
    new_names = []
    
    for col in features.columns:
        new_names.append('track_id' if col.lower() in ['msd_trackid', 'track_id'] else col)

    # replace with new names
    renamed_features = features.toDF(*new_names)

    return renamed_features
