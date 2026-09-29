from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "k").orderBy(F.col("latency_ms").desc(), "id", "k")
w2 = Window.orderBy("rn", "id", "k")
w3 = Window.partitionBy("host").orderBy("log_ts", "id", "k").rowsBetween(-5, Window.currentRow)
w_recent = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w5 = Window.partitionBy("k").orderBy("status_code_plus_latency_ms", "id", "k").rowsBetween(-3, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

valid = (
    exploded
    .filter(F.col("log_ts") >= F.lit("2025-06-01").cast("date"))
    .withColumn("k_group", F.when(F.col("k").isin(["trace", "version"]), "Group A").otherwise("Other"))
)

latest = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

base = (
    latest
    .filter(F.col("k").isNotNull() & F.hour("log_ts").between(12, 18))
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        "k",
        "value",
        "k_group",
        "rn",
        (F.col("status_code") + F.col("latency_ms")).alias("status_code_plus_latency_ms"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.lead("latency_ms", 1).over(w2).alias("next_latency_ms"),
        F.avg("status_code").over(w3).alias("status_code_avg_5"),
        F.avg("status_code").over(w_recent).alias("status_code_avg_15m"),
    )
)

enriched = (
    metrics
    .filter(F.col("status_code_avg_5") == 402)
    .withColumn("last4_min_status_code_avg_5", F.min("status_code_avg_5").over(w5))
    .withColumn("last4_count_status_code_avg_5", F.count("status_code_avg_5").over(w5))
    .withColumn("last4_max_status_code_avg_5", F.max("status_code_avg_5").over(w5))
)

result = (
    enriched
    .select(
        "id",
        "host",
        "k",
        "k_group",
        "status_code_plus_latency_ms",
        "next_latency_ms",
        "status_code_avg_5",
        "status_code_avg_15m",
    )
    .orderBy("host")
)
