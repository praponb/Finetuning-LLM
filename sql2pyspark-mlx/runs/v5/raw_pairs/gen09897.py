from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select("id", "host", "log_ts", "level", F.explode_outer("context").alias("k", "v"))
)

result = (
    flattened
    .groupBy(
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
    )
    .agg(
        F.countDistinct("k").alias("distinct_ks"),
        F.count_if(F.col("k") == "version").alias("version_events"),
        F.size(F.collect_set("k")).alias("num_ks"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.countDistinct("k").alias("distinct_ks_2"),
    )
)
