from botocore.configprovider import BaseProvider
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, count, col, desc, countDistinct, udf
from pyspark.sql.types import BooleanType
import boto3
from botocore.config import Config
import re
import os

required_fields = [ "subject", "grade" ]
file_type_regex = r".(*)$"
uni_name_regex = r"data/(*).$"

config = Config(region_name="eu-west-1")
s3 = boto3.client("s3", config=config)

pass_grades = ["A+", "A", "A-", "B+", "B", "B-", "C+", "C", "C-"]

def find_most_popular_subjects(df):
    return df.groupBy('subject').count().sort(desc("count")).head(3)

def find_unique_subjects(df):
    unique_subjects = df.groupBy('subject').agg(countDistinct("university").alias("uni_count")).filter(col("uni_count") == 1).select("subject").select("subject").rdd.flatMap(lambda x: x).collect()
    return df.filter(col("subject").isin(unique_subjects)).groupby(["university", "subject"]).count()

def find_pass_rate_per_university(df):
    return df.groupby(["university", "total_enrolments"]).agg(count(when(col("course_passed") == True, True))).withColumnRenamed('count(CASE WHEN (course_passed = true) THEN true END)', 'pass_count').withColumn('prop_of_total', col('pass_count')/col("total_enrolments"))

def read_file_to_df(spark, s3_bucket, filepath):
    base_file_path = f"s3a://{s3_bucket}/"
    file_type = re.search(file_type_regex, filepath).group(1)
    df = spark.read.format(file_type).load(base_file_path + filepath).select(*required_fields)
    uni_name = re.search(uni_name_regex, filepath).group(1)
    return df.withColumn("university", uni_name).withColumn("total_enrolments", df.count())

def calculate_pass(x):
    try:
        x = int(x)
        if x >= 50:
            return True
        return False
    except:
        if x in pass_grades:
            return True
        return False

calculate_pass_udf = udf(calculate_pass, BooleanType())

def add_pass_col(df): 
    return df.withColumn("course_passed", calculate_pass_udf(col("grade")))

def get_file_paths(s3_bucket):
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=s3_bucket, Prefix="data")
    for page in pages:
        if "Contents" in page:
            keys += [ obj["Key"] for obj in page['Contents'] if obj["Key"].rindex("/") != len(obj["Key"]) - 1 ]
    return keys

def get_spark_session():
    return (
        SparkSession.builder.appName("uni_data_analysis")
        .config("spark.executorEnv.PYTHONHASHSEED", "0")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

def process_files():
    s3_bucket = os.getenv('S3_BUCKET')
    #  get all filepaths to be analysed
    file_paths = get_file_paths(s3_bucket)

    # get spark session
    spark = get_spark_session()


    df = spark.createDataFrame([], required_fields + ["university", "total_enrolments"])

    # load each data file into same dataframe
    for filepath in file_paths:
        uni_df = read_file_to_df(spark, s3_bucket, filepath)
        df_with_pass = add_pass_col(uni_df)
        df = df.union(df_with_pass)

    # run analysis on full datafame 
    most_popular_courses_df = find_most_popular_subjects(df)
    most_popular_courses_df.show()
    unique_subjects_df = find_unique_subjects(df)
    unique_subjects_df.show()
    pass_rate_df = find_pass_rate_per_university(df)
    pass_rate_df.show()
    
if __name__ == "__main__":
    process_files()