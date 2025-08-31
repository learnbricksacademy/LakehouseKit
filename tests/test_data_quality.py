import pytest

# -------------------------
# Raw Layer Quality Tests
# -------------------------

def test_raw_policies_no_null_ids(spark):
    """Ensure primary key columns in raw.policies are not null"""
    df = spark.table("raw.policies")
    assert df.filter("PolicyID IS NULL OR CustID IS NULL").count() == 0, "Null PolicyID or CustID found in raw.policies"


# -------------------------
# Harmonize Layer Quality Tests
# -------------------------

def test_harmonize_schema_consistency(spark):
    """Check that harmonize.policies_standardized has expected schema"""
    df = spark.table("harmonize.policies_standardized")
    expected = {"policy_id", "customer_id", "policy_date", "premium_amount", "status"}
    assert expected.issubset(set(df.columns)), f"Harmonized schema mismatch: {df.columns}"

def test_harmonize_premium_non_negative(spark):
    """Check that premium_amount is never negative"""
    df = spark.table("harmonize.policies_standardized")
    assert df.filter("premium_amount < 0").count() == 0, "Negative premium amounts found"


# -------------------------
# Refined Layer Quality Tests
# -------------------------

def test_refined_policy_kpis_not_empty(spark):
    """Ensure KPI table has data"""
    df = spark.table("refined.policy_kpis")
    assert df.count() > 0, "Refined policy_kpis table is empty"

def test_refined_customer360_expected_columns(spark):
    """Ensure Customer360 view contains expected metrics"""
    df = spark.table("refined.customer360")
    expected_cols = {"customer_id", "customer_name", "customer_segment", "total_policies", "total_premium"}
    assert expected_cols.issubset(set(df.columns)), f"Refined.customer360 schema mismatch: {df.columns}"

def test_refined_customer360_no_negative_premium(spark):
    """Ensure no negative premium totals in Customer360"""
    df = spark.table("refined.customer360")
    assert df.filter("total_premium < 0").count() == 0, "Negative total_premium found in customer360"
