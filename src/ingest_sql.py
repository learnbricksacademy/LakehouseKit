def ingest_sql(spark, src, dbutils):
    username = dbutils.secrets.get(scope="sql-secrets", key=src["username_secret"])
    password = dbutils.secrets.get(scope="sql-secrets", key=src["password_secret"])

    jdbcUrl = f"jdbc:sqlserver://{src['jdbc_hostname']}:{src['jdbc_port']};database={src['database']};encrypt=true;trustServerCertificate=false;loginTimeout=30;"

    return spark.read.format("jdbc") \
        .option("url", jdbcUrl) \
        .option("dbtable", src["table"]) \
        .option("user", username) \
        .option("password", password) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()
