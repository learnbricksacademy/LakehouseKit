import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("test-transformations").getOrCreate()

def test_harmonize_table_exists(spark):
    """Check harmonized tables exist"""
    tables = [t.name for t in spark.catalog.listTables("harmonize")]
    assert "policies_standardized" in tables, "Harmonize table 'policies_standardized' does not exist"

def test_column_standardization(spark):
    """Check column standardization rules applied"""
    df = spark.table("harmonize.policies_standardized")
    expected_cols = {"policy_id", "customer_id", "premium_amount", "policy_date"}
    assert expected_cols.issubset(set(df.columns)), "Harmonize schema missing expected columns"

def test_no_duplicates_in_harmonize(spark):
    """Ensure duplicates are dropped in harmonized data"""
    df = spark.table("harmonize.policies_standardized")
    assert df.count() == df.dropDuplicates().count(), "Duplicate rows found in harmonized table"
