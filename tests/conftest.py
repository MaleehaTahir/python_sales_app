import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder\
        .master("local[16]")\
        .appName("pytest_test")\
        .config("spark.executorEnv.PYTHONHASHSEED", "0")\
        .getOrCreate()
    return spark

