from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

result = (
    flattened
    .groupBy(F.date_trunc("DAY", "log_ts").alias("activity_day"))
    .agg(
        F.size(F.collect_set("k")).alias("num_ks"),
        F.stddev_samp("status_code").alias("status_code_stddev"),
    )
    .filter(F.col("num_ks") >= 1)
    .orderBy(F.col("num_ks").desc())
)
