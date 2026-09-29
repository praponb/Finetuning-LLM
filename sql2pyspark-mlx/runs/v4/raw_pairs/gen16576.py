from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("service").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("k").orderBy("prev_log_ts", "id", "k")
w4 = Window.partitionBy("k").orderBy("prev_log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("service")
w6 = Window.partitionBy("service", "host").orderBy("prev_log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("k").orderBy("latency_ms")
w8 = Window.partitionBy("k").orderBy(F.col("service_avg_latency_ms").desc(), "id", "k")
w9 = Window.partitionBy("service").orderBy("session_no", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

valid = (
    exploded
    .filter(F.col("k").isNotNull() & F.col("k").isin(["trace", "region"]))
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        "k",
        "v",
        F.round((F.col("latency_ms") / F.col("status_code")), 2).alias("latency_ms_per_status_code"),
        F.coalesce("k", F.lit("UNKNOWN")).alias("resolved_k"),
    )
)

with_prev = valid.withColumn("prev_log_ts", F.lag("log_ts").over(w))

gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

prev_events = sessionized.withColumn("prev_prev_log_ts", F.lag("prev_log_ts").over(w3))

gaps_2 = (
    prev_events
    .withColumn("is_new_burst_2", F.when(F.col("prev_prev_log_ts").isNull() | (F.col("prev_log_ts") > F.col("prev_prev_log_ts") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = gaps_2.withColumn("burst_id", F.sum("is_new_burst_2").over(w4))

enriched = (
    islands
    .withColumn("service_avg_latency_ms", F.avg("latency_ms").over(w5))
    .withColumn("first_known_latency_ms_per_status_code", F.first("latency_ms_per_status_code", ignorenulls=True).over(w6))
)

latest = (
    enriched
    .withColumn("rank_in_group", F.rank().over(w7))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .select(
        "*",
        F.ntile(4).over(w8).alias("service_avg_latency_ms_bucket"),
        F.last("is_new_burst", ignorenulls=True).over(w9).alias("last_known_is_new_burst"),
    )
)
