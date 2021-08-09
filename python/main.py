from pyspark.sql import SparkSession
from pyspark.sql.functions import when, count, col, desc, countDistinct, lit, udf, trim
from pyspark.sql.types import BooleanType, StructType, StringType, StructField
import boto3
from botocore.config import Config
import re
import os
import json

regex = r'data/([A-Za-z0-9\-]*).([A-Za-z0-9\-]*)'

config = Config(region_name='eu-west-1')
s3 = boto3.client('s3', config=config)

pass_grades = ['A+', 'A', 'A-', 'B+', 'B', 'B-', 'C+', 'C', 'C-']
req_fields = ["subject", "grade"]

def find_most_popular_subjects(df):
    return df.groupBy('subject').count().sort(desc('count')).head(3)

def find_unique_subjects(df):
    unique_subjects = df.groupBy('subject').agg(countDistinct('university').alias('uni_count')).filter(col('uni_count') == 1).select('subject').select('subject').rdd.flatMap(lambda x: x).collect()
    return df.filter(col('subject').isin(unique_subjects)).groupby(['university', 'subject']).count()

def find_pass_rate_per_university(df):
    return df.groupby(['university', 'total_enrolments']).agg(count(when(col('course_passed') == True, True))).withColumnRenamed('count(CASE WHEN (course_passed = true) THEN true END)', 'pass_count').withColumn('prop_of_total', col('pass_count')/col('total_enrolments'))

def read_file_to_df(spark, s3_bucket, filepath):
    base_file_path = f's3a://{s3_bucket}/'
    file_type = re.search(regex, filepath).group(2)
    if file_type == 'csv':
        df = spark.read.option('header',True).format('csv').load(base_file_path + filepath)
        df = df.select([col(cl).alias(cl.replace(' ', '')) for cl in df.columns]).select(req_fields)
    else:
        df = spark.read.option("multiline",True).option("primitivesAsString", True).format('json').load(base_file_path + filepath).select(req_fields)
    for field in req_fields:
        df = df.withColumn(field, trim(col(field)))
    df = df.filter(col("subject") != "subject")
    uni_name = re.search(regex, filepath).group(1)
    return df.withColumn('university', lit(uni_name)).withColumn('total_enrolments', lit(df.count()))

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

    return df.withColumn('course_passed', calculate_pass_udf(col('grade')))

def get_file_paths(s3_bucket):
    return ["data/uni1.csv", "data/uni2.csv", "data/uni3.json"]
    # keys = []
    # , 
    # paginator = s3.get_paginator('list_objects_v2')
    # pages = paginator.paginate(Bucket=s3_bucket, Prefix='data')
    # for page in pages:
    #     if 'Contents' in page:
    #         keys += [ obj['Key'] for obj in page['Contents'] if obj['Key'].rindex('/') != len(obj['Key']) - 1 ]
    # return keys

def get_spark_session():
    spark = (
        SparkSession.builder.appName('uni_data_analysis')
        .getOrCreate()
    )
    return spark

def process_files():
    s3_bucket = os.getenv('S3_BUCKET')
    # aws_access_key = os.getenv('AWS_ACCESS_KEY')
    # aws_secret_key = os.getenv('AWS_SECRET_KEY')

    # get spark session
    spark = get_spark_session()

    #  get all filepaths to be analysed
    file_paths = get_file_paths(s3_bucket)
    schema = StructType([
        StructField('subject', StringType()),
        StructField('grade', StringType()),
        StructField('university', StringType()),
        StructField('total_enrolments', StringType()),
        StructField('course_passed', BooleanType())
    ])
    
    df = spark.createDataFrame([], schema)

    # load each data file into same dataframe
    for filepath in file_paths:
        uni_df = read_file_to_df(spark, s3_bucket, filepath)
        df_with_pass = add_pass_col(uni_df)
        df_with_pass.show()
        df = df.union(df_with_pass)

    # run analysis on full datafame 
    most_popular_courses = find_most_popular_subjects(df)
    print(most_popular_courses)
    print("popuar?")
    unique_subjects_df = find_unique_subjects(df)
    print('here?')
    print(unique_subjects_df.count())
    print(unique_subjects_df.collect())
    print("pass_rate")
    pass_rate_df = find_pass_rate_per_university(df)
    print(pass_rate_df.collect())

    spark.stop()
    
if __name__ == '__main__':
    process_files()