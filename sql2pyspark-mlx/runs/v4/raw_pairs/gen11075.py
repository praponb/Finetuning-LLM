from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        F.explode("context").alias("attr_key", "v"),
    )
)

result = (
    lines
    .groupBy(F.date_trunc("DAY", "log_ts").alias("activity_day"))
    .agg(
        F.sort_array(F.collect_set("host")).alias("host_list"),
        F.count("*").alias("cnt"),
        F.size(F.collect_set("host")).alias("num_hosts"),
        F.min("log_ts").alias("first_seen"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows"),
    )
)
