from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        F.explode("context").alias("k", "v"),
    )
)

result = (
    flattened
    .rollup("k")
    .agg(
        F.count(F.when(F.col("level") == "ERROR", 1)).alias("error_rows"),
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_count"),
        F.max("log_ts").alias("last_log_ts"),
        F.grouping("k").alias("grouping_id"),
    )
    .filter(F.col("debug_count") > 1)
    .orderBy(F.col("k").desc())
)
