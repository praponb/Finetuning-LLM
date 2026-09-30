from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key", "host")
w2 = Window.partitionBy("level")
w3 = Window.partitionBy("service")
w4 = Window.partitionBy("service").orderBy("latency_ms", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("key").orderBy("log_ts", "id", "key")
w_part = Window.partitionBy("service").orderBy("status_code", "id", "key").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy("level", "service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w8 = Window.partitionBy("key").orderBy("status_code", "id", "key").rowsBetween(-6, Window.currentRow)
w9 = Window.orderBy("last4_max_running_latency_ms", "id", "key").rowsBetween(-6, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

metrics = (
    flattened
    .filter(F.col("log_ts") < "2024-04-22")
    .withColumn("corr_status_code_latency_ms", F.corr("status_code", "latency_ms").over(w))
    .withColumn("level_avg_latency_ms", F.avg("latency_ms").over(w2))
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4))
    .withColumn("running_latency_ms", F.sum("latency_ms").over(w4))
)

deduped = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

metrics_app_logs = (
    deduped
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        "key",
        "corr_status_code_latency_ms",
        "latency_ms_share",
        "running_latency_ms",
        "dedup_rank",
        F.max("running_latency_ms").over(w_part).alias("last4_max_running_latency_ms"),
        F.sum("running_latency_ms").over(w_part).alias("last4_sum_running_latency_ms"),
        F.count("running_latency_ms").over(w_part).alias("last4_count_running_latency_ms"),
        F.avg("running_latency_ms").over(w_part).alias("last4_avg_running_latency_ms"),
        F.sum("level_avg_latency_ms").over(w7).alias("level_avg_latency_ms_sum_15m"),
    )
)

scored = (
    metrics_app_logs
    .select(
        "id",
        "latency_ms",
        "status_code",
        "key",
        "latency_ms_share",
        "running_latency_ms",
        "last4_count_running_latency_ms",
        "level_avg_latency_ms_sum_15m",
        F.count("latency_ms_share").over(w8).alias("last7_count_latency_ms_share"),
        F.max("latency_ms_share").over(w8).alias("last7_max_latency_ms_share"),
        F.sum("latency_ms_share").over(w8).alias("last7_sum_latency_ms_share"),
        F.min("latency_ms_share").over(w8).alias("last7_min_latency_ms_share"),
        F.avg("last4_count_running_latency_ms").over(w9).alias("last4_count_running_latency_ms_avg_6"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("last7_min_latency_ms_share") >= 0.89, "PREMIUM").otherwise("STANDARD").alias("last7_min_latency_ms_share_tier"),
    )
    .agg(
        F.count(F.when(F.col("key") == "version", 1)).alias("version_count"),
        F.count_if(F.col("key") == "region").alias("region_events"),
    )
)
