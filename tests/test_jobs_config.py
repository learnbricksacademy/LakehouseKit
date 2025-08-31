import json

def test_jobs_json_valid():
    """Ensure jobs.json structure is correct."""
    with open(".devops/jobs.json", "r") as f:
        jobs = json.load(f)

    assert "jobs" in jobs
    for job in jobs["jobs"]:
        assert "name" in job
        assert "notebook_task" in job or "spark_python_task" in job
        assert "schedule" in job
