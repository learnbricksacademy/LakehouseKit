import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("databricks-framework-tests")
        .getOrCreate()
    )

    spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # Load test data from SQL file
    with open("tests/test_data_setup.sql", "r") as f:
        sql_commands = f.read().split(";")
        for cmd in sql_commands:
            if cmd.strip():
                spark.sql(cmd)

    yield spark
    spark.stop()
