from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_code")
w2 = Window.partitionBy("service")
w_recent = Window.partitionBy("attr_key").orderBy(F.col("status_code_share").desc_nulls_last())
w4 = Window.partitionBy("attr_key")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

with_windows = (
    flattened
    .withColumn("status_code_cume_dist", F.cume_dist().over(w))
    .withColumn("status_code_share", F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4))
)

with_windows_app_logs = (
    with_windows
    .withColumn("service_min_status_code_cume_dist", F.min("status_code_cume_dist").over(w2))
    .withColumn("status_code_share_rank", F.rank().over(w_recent))
    .withColumn("status_code_share_2", F.round((F.col("status_code") / F.sum("status_code").over(w4)), 4))
)

result = (
    with_windows_app_logs
    .groupBy(F.date_trunc("DAY", "log_ts").alias("activity_day"))
    .agg(
        F.percentile_approx("status_code_share_rank", [0.25, 0.5, 0.75]).alias("status_code_share_rank_quartiles"),
        F.round(F.avg("status_code_cume_dist"), 2).alias("avg_status_code_cume_dist"),
        F.size(F.collect_set("service")).alias("num_services"),
    )
)
