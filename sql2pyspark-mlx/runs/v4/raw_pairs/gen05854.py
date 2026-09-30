from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("latency_ms_whole").desc_nulls_last())
w2 = Window.orderBy("status_code")
w3 = Window.partitionBy("key")

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

filtered = (
    exploded
    .filter(F.col("key").isin(["region", "version", "trace"]))
    .withColumn("latency_ms_whole", F.floor("latency_ms"))
    .withColumn("resolved_key", F.coalesce("key", F.lit("UNKNOWN")))
)

with_windows = (
    filtered
    .withColumn("latency_ms_whole_cume_dist", F.cume_dist().over(w))
    .withColumn("status_code_percent_rank", F.percent_rank().over(w2))
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4))
)

result = (
    with_windows
    .select(
        "id",
        "host",
        "level",
        "status_code",
        "key",
        "latency_ms_whole",
        "resolved_key",
        "latency_ms_whole_cume_dist",
        "latency_ms_share",
    )
    .orderBy(F.col("latency_ms_share").desc_nulls_first())
)
