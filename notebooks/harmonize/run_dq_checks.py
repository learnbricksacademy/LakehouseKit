from src.utils import load_config
from src.harmonize.dq_engine import run_dq_checks
import json

# ---------------- Params ----------------
dbutils.widgets.text("dq_config", "/Workspace/Repos/databricks-data-migration-framework/configs/harmonize/dq_expectations.yaml")
dq_config_file = dbutils.widgets.get("dq_config")

# ---------------- Load Config ----------------
config = load_config(dq_config_file)
expectations = config["expectations"]

for exp in expectations:
    print(f"🚀 Running DQ checks: {exp['name']} on {exp['table']}")
    results = run_dq_checks(spark, exp)

    failed = [r for r in results if not r["success"]]
    if failed:
        print(f"❌ Data Quality FAILED for {exp['table']}")
        print(json.dumps(failed, indent=2))
        # Optionally: write to system.transformation_validations
        log_data = [(exp["name"], json.dumps(failed))]
        df = spark.createDataFrame(log_data, ["dq_name", "results"])
        df.write.format("delta").mode("append").saveAsTable("system.transformation_validations")
        raise Exception(f"DQ failed for {exp['table']}")
    else:
        print(f"✅ DQ PASSED for {exp['table']}")
