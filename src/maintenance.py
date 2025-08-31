from src.notifier import send_notification

def safe_execute(spark, fn, args, task_name, dbutils, config):
    try:
        fn(spark, *args)
        send_notification(dbutils, f"✅ {task_name} succeeded", config)
    except Exception as e:
        error_msg = f"❌ {task_name} failed: {str(e)}"
        print(error_msg)
        send_notification(dbutils, error_msg, config)
        raise

def optimize_and_zorder(spark, table_name, zorder_cols=[]):
    if zorder_cols:
        zorder_str = ", ".join(zorder_cols)
        sql = f"OPTIMIZE {table_name} ZORDER BY ({zorder_str})"
    else:
        sql = f"OPTIMIZE {table_name}"
    spark.sql(sql)

def apply_retention_policy(spark, table_name, column, interval):
    sql = f"""
    DELETE FROM {table_name}
    WHERE {column} < current_timestamp() - INTERVAL {interval}
    """
    spark.sql(sql)

def vacuum_table(spark, table_name, hours=168):
    sql = f"VACUUM {table_name} RETAIN {hours} HOURS"
    spark.sql(sql)
