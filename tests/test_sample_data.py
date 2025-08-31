import os
import pandas as pd

def test_sample_data_exists():
    """Check if sample data files exist for onboarding."""
    assert os.path.exists("sample_data/customers.csv")
    assert os.path.exists("sample_data/transactions.json")

def test_sample_data_quality():
    """Validate structure of sample CSV."""
    df = pd.read_csv("sample_data/customers.csv")
    assert not df.empty
    assert "customer_id" in df.columns
    assert df["customer_id"].is_unique
