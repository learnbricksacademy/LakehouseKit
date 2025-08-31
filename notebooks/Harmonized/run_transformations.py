from src.utils import load_config
from src.transformations.base_engine import TransformationEngine

# ---------------- Params ----------------
dbutils.widgets.text("config_file", "/Workspace/Repos/databricks-data-migration-framework/configs/transformations.yaml")
dbutils.widgets.text("transformation_name", "")
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("container", "")

config_file = dbutils.widgets.get("config_file")
transformation_name = dbutils.widgets.get("transformation_name")
base_path = f"{dbutils.widgets.get('container')}@{dbutils.widgets.get('storage_account')}.dfs.core.windows.net"

config = load_config(config_file)
engine = TransformationEngine(spark, base_path)

all_transforms = config["transformations"]
transforms = [t for t in all_transforms if not transformation_name or t["name"] == transformation_name]

for t in transforms:
    print(f"🚀 Running transformation: {t['name']}")
    engine.run(t)
