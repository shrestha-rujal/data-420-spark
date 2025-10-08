from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import getpass
import pandas
import pyspark
import random
import re
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

# WIP - docstrings will be added at the end if sufficient time is remaining for a final refactoring

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

# helper function to train test split in a stratified manner
def stratified_split(df, label_col, train_fraction=0.8, seed=42):
    
    window_spec = Window.partitionBy(label_col).orderBy(F.rand(seed))    
    df_ranked = df.withColumn("row_number", F.row_number().over(window_spec))
    class_counts = df_ranked.groupBy(label_col).agg(F.count("*").alias("n")).collect()
    cutoffs = {row[label_col]: int(row["n"] * train_fraction) for row in class_counts}

    # combine all conditions to help label train rows
    condition = None
    for cls, cutoff in cutoffs.items():
        cond = ((F.col(label_col) == cls) & (F.col("row_number") <= cutoff))
        condition = cond if condition is None else (condition | cond)

    # Add is_train flag
    df_flagged = df_ranked.withColumn("is_train", F.when(condition, True).otherwise(False))

    # Split
    train_df = df_flagged.filter(F.col("is_train") == True).drop("row_number", "is_train")
    test_df = df_flagged.filter(F.col("is_train") == False).drop("row_number", "is_train")

    return train_df, test_df

# function to calculate class weights using Inverse Frequency Weights
# returns the df with added weight column
def apply_class_weights(df, label_col="label", weight_col="weight"):    
    class_counts = df.groupBy(label_col).count().collect()
    total = df.count()
    num_classes = len(class_counts)

    # inverse frequency weights formula
    weights = {row[label_col]: total / (num_classes * row["count"]) for row in class_counts}

    # **complex code**
    # flatten the weight dictionary to be used as literals in create_map suitable for use with spark dataframe
    weight_map = F.create_map([F.lit(x) for kv in weights.items() for x in kv])


    df_weighted = df.withColumn(weight_col, weight_map[F.col(label_col)])

    return df_weighted

# calculate evaluation metrics for multiclass predictions
def get_multiclass_metrics(predictions, label_col="label"):
    evaluator = MulticlassClassificationEvaluator(labelCol=label_col, predictionCol="prediction")

    accuracy = evaluator.setMetricName("accuracy").evaluate(predictions)
    f1 = evaluator.setMetricName("f1").evaluate(predictions)
    precision = evaluator.setMetricName("weightedPrecision").evaluate(predictions)
    recall = evaluator.setMetricName("weightedRecall").evaluate(predictions)

    return {
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1": f1
    }








