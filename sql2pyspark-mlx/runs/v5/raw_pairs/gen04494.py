from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "k")
w2 = Window.partitionBy("level")
w3 = Window.partitionBy("k", "level").orderBy("status_code_share")
w4 = Window.partitionBy("status_code_share_dense_rank_tier").orderBy(F.col("error_events").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

with_windows = (
    lines
    .filter(F.col("level") == "WARN")
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        "k",
        "value",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
    )
)

with_windows_app_logs = (
    with_windows
    .select(
        "id",
        "service",
        "level",
        F.corr("status_code", "latency_ms").over(w2).alias("corr_status_code_latency_ms"),
        F.dense_rank().over(w3).alias("status_code_share_dense_rank"),
    )
)

base = (
    with_windows_app_logs
    .filter(F.col("level").isin(["ERROR", "DEBUG"]))
    .withColumn("level_lower", F.lower("level"))
)

result = (
    base
    .groupBy(
        F.when(F.col("status_code_share_dense_rank") >= 7, "HIGH").otherwise("LOW").alias("status_code_share_dense_rank_tier"),
        "service",
    )
    .agg(
        F.round(F.avg("corr_status_code_latency_ms"), 2).alias("avg_corr_status_code_latency_ms"),
        F.avg(F.when(F.col("service") == "checkout", F.col("corr_status_code_latency_ms"))).alias("checkout_avg_corr_status_code_latency_ms"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("level") == "ERROR").alias("error_events"),
    )
    .withColumn("error_events_percent_rank", F.percent_rank().over(w4))
)
