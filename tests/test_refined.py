import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("test-refined").getOrCreate()

def test_refined_kpis(spark):
    """Check that KPI table exists and has data"""
    tables = [t.name for t in spark.catalog.listTables("refined")]
    assert "policy_kpis" in tables, "Refined table 'policy_kpis' does not exist"

    df = spark.table("refined.policy_kpis")
    assert df.count() > 0, "Refined KPI table is empty"

def test_refined_customer360(spark):
    """Check Customer360 table has required metrics"""
    df = spark.table("refined.customer360")
    assert "total_policies" in df.columns
    assert "total_premium" in df.columns
