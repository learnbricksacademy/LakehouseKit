from src.utils import load_config
from src.landing.file_loader import load_file_to_raw

dbutils.widgets.text("config_file", "/Workspace/Repos/databricks-data-migration-framework/configs/landing/landing_sources.yaml")
dbutils.widgets.text("source_name", "")

config_file = dbutils.widgets.get("config_file")
source_name = dbutils.widgets.get("source_name")

config = load_config(config_file)
all_sources = config["landing_sources"]

if source_name:
    sources = [s for s in all_sources if s["name"] == source_name]
else:
    sources = all_sources

for s in sources:
    print(f"🚀 Loading landing source: {s['name']}")
    load_file_to_raw(spark, s, "landing")
