from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        "context",
        F.col("context")["region"].alias("region"),
        F.col("context").getItem("version").alias("version"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
        F.col("context").getItem("trace").alias("trace"),
    )
)
