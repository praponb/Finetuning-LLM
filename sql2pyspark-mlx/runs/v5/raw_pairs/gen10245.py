from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        F.explode("context").alias("key", "attr_value"),
    )
)

result = (
    flattened
    .cube("level")
    .agg(
        F.count(F.when(F.col("level") == "WARN", 1)).alias("warn_count"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.count(F.when(F.col("level") == "INFO", 1)).alias("info_count"),
        F.grouping("level").alias("grouping_id"),
    )
    .orderBy(F.col("level").desc())
)
