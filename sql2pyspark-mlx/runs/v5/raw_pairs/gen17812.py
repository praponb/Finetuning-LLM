from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        F.explode("context").alias("attr_key", "value"),
    )
)

result = (
    lines
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"))
    .agg(
        F.countDistinct("level").alias("distinct_levels"),
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
        F.count(F.when(F.col("attr_key") == "version", 1)).alias("version_count"),
    )
)
