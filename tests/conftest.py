import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    active = SparkSession.getActiveSession()
    if active is not None:  # on Databricks: reuse the notebook session and never stop it
        yield active
        return
    session = (
        SparkSession.builder.master("local[1]")
        .appName("alm-etl-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.ansi.enabled", "true")  # same behaviour as Databricks serverless
        .getOrCreate()
    )
    yield session
    session.stop()
