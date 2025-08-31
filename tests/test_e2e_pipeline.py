import pytest
import subprocess

def run_notebook(notebook_path: str):
    """Run a Databricks notebook via Databricks CLI (local test stub)."""
    result = subprocess.run(
        ["databricks", "workspace", "export_dir", notebook_path, "/tmp/test_run"],
        capture_output=True, text=True
    )
    return result.returncode, result.stdout, result.stderr

def test_e2e_pipeline():
    # Run landing ingestion
    code, out, err = run_notebook("notebooks/landing/ingest_files.py")
    assert code == 0, f"Ingestion failed: {err}"

    # Run harmonize transformations
    code, out, err = run_notebook("notebooks/Harmonize/run_transformations.py")
    assert code == 0, f"Transformation failed: {err}"

    # Run refined consumption
    code, out, err = run_notebook("notebooks/refined/run_consumption.py")
    assert code == 0, f"Refined step failed: {err}"
