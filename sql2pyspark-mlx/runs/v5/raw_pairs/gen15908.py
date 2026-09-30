from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        F.explode("context").alias("attr_key", "value"),
    )
)

result = (
    flattened
    .groupBy(F.date_trunc("DAY", "log_ts").alias("activity_day"))
    .agg(
        F.count("*").alias("num_rows"),
        F.min("log_ts").alias("started_at"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
    )
    .orderBy(F.col("num_attr_keys").asc_nulls_first())
)
