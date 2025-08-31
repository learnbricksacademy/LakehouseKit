import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("test-ingestion").getOrCreate()

def test_raw_table_exists(spark):
    """Check that raw tables are created after ingestion"""
    tables = [t.name for t in spark.catalog.listTables("raw")]
    assert "policies" in tables, "Raw table 'policies' does not exist"
    assert "customers" in tables, "Raw table 'customers' does not exist"

def test_raw_table_not_empty(spark):
    """Check that raw tables are not empty"""
    df = spark.table("raw.policies")
    assert df.count() > 0, "Raw policies table is empty"
