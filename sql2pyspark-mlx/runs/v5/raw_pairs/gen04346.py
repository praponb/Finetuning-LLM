from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("log_ts")
w2 = Window.partitionBy("k").orderBy(F.col("rn").desc(), "id", "k")
w3 = Window.partitionBy("k").orderBy("log_ts", "id", "k")
w4 = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("rn", "id", "k")
w6 = Window.partitionBy("k").orderBy("burst_id", "id", "k")
w_running = Window.partitionBy("k").orderBy("burst_id_bucket", "id", "k")
w8 = Window.partitionBy("k")
w_hist = Window.partitionBy("k").orderBy(F.col("burst_id_bucket").desc(), "id", "k")

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("k", "value"),
    )
)

latest = (
    flattened
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

ranked = (
    latest
    .withColumn("rn_2", F.row_number().over(w2))
    .filter(F.col("rn_2") == 1)
)

gaps = (
    ranked
    .withColumn("new_group_flag", F.when(F.col("log_ts") - F.lag("log_ts").over(w3) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessions_app_logs = (
    gaps
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "k",
        "value",
        "rn",
        "rn_2",
        F.sum("new_group_flag").over(w4).alias("burst_id"),
    )
)

scored = (
    sessions_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "k",
        "value",
        "rn",
        "rn_2",
        F.lead("latency_ms", 1).over(w5).alias("next_latency_ms"),
        F.ntile(5).over(w6).alias("burst_id_bucket"),
    )
)

result = (
    scored
    .filter(F.col("rn") >= 1)
    .withColumn("burst_id_bucket_bucket", F.ntile(3).over(w_running))
    .withColumn("k_max_next_latency_ms", F.max("next_latency_ms").over(w8))
    .withColumn("burst_id_bucket_bucket_2", F.ntile(5).over(w_hist))
)
