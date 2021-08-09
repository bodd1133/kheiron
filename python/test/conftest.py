import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
    return spark