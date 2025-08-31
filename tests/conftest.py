import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """
    Create a Spark session for all tests (shared across test files).
    Scope=session → only starts once per test run.
    """
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("databricks-framework-tests")
        .getOrCreate()
    )

    # Ensure Delta support is enabled for local tests
    spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    yield spark

    # Teardown after tests
    spark.stop()
