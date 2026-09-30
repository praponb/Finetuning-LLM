from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w_running = Window.partitionBy("host").orderBy(F.col("status_code").desc())
w3 = Window.orderBy("status_code")
w4 = Window.partitionBy("k")
w5 = Window.partitionBy("host", "k")
w6 = Window.orderBy(F.col("status_code").desc())
w7 = Window.orderBy(F.col("status_code_cume_dist").asc_nulls_last())
w8 = Window.partitionBy("k", "host").orderBy(F.col("status_code_cume_dist").desc_nulls_last())

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

with_windows = (
    lines
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4))
    .withColumn("status_code_percent_rank", F.percent_rank().over(w_running))
    .withColumn("status_code_cume_dist", F.cume_dist().over(w3))
)

with_windows_app_logs = (
    with_windows
    .select(
        "*",
        F.rank().over(w3).alias("status_code_rank"),
        F.corr("latency_ms", "status_code").over(w4).alias("corr_latency_ms_status_code"),
    )
)

scored = (
    with_windows_app_logs
    .select(
        "*",
        F.cume_dist().over(w_running).alias("status_code_cume_dist_2"),
        F.sum("status_code_cume_dist").over(w5).alias("host_sum_status_code_cume_dist"),
        F.dense_rank().over(w6).alias("status_code_dense_rank"),
        F.percent_rank().over(w7).alias("status_code_cume_dist_percent_rank"),
        F.round((F.col("status_code_rank") / F.sum("status_code_rank").over(w5)), 4).alias("status_code_rank_share"),
    )
)

enriched = (
    scored
    .withColumn("status_code_cume_dist_cume_dist", F.cume_dist().over(w8))
    .withColumn("host_max_status_code_percent_rank", F.max("status_code_percent_rank").over(w))
)

result = (
    enriched
    .groupBy("host")
    .pivot("k", ["region", "version", "trace"])
    .agg(F.sum("status_code_rank_share").alias("amount"), F.count(F.lit(1)).alias("n"))
)
