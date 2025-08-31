from src.utils import load_config
from src.refined.consumption import apply_consumption

# ---------------- Params ----------------
dbutils.widgets.text("config_file", "/Workspace/Repos/databricks-data-migration-framework/configs/refined/consumption.yaml")
dbutils.widgets.text("job_name", "")

config_file = dbutils.widgets.get("config_file")
job_name = dbutils.widgets.get("job_name")

config = load_config(config_file)
jobs = config["consumption"]

if job_name:
    jobs = [j for j in jobs if j["name"] == job_name]

for job in jobs:
    print(f"🚀 Running refined job: {job['name']}")
    apply_consumption(spark, job)
