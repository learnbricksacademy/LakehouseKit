import yaml

def load_config(config_file):
    with open(config_file, "r") as f:
        return yaml.safe_load(f)

def get_checkpoint_path(base_path, schema, table):
    return f"abfss://{base_path}/checkpoints/{schema}/{table}"

def get_table_path(base_path, schema, table):
    return f"abfss://{base_path}/{schema}/{table}"
