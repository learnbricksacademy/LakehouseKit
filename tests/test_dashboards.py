import glob
import json

def test_dashboards_are_valid_json():
    """Ensure all dashboards JSON files are valid."""
    for file in glob.glob("dashboards/**/*.json", recursive=True):
        with open(file, "r") as f:
            try:
                json.load(f)
            except Exception as e:
                raise AssertionError(f"Invalid JSON in {file}: {e}")
