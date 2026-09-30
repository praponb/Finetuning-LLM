from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("service").orderBy("latency_ms_whole", "id", "key").rowsBetween(-2, Window.currentRow)
w_part_2 = Window.partitionBy("key")
w3 = Window.partitionBy("key").orderBy(F.col("latency_ms_whole").desc())

flattened = (
    df_app_logs
    .select("id", "service", "latency_ms", F.explode("context").alias("key", "value"))
)

base = (
    flattened
    .filter(F.col("key").isNotNull())
    .select("*", F.length("key").alias("key_length"))
)

valid = (
    base
    .filter(F.col("latency_ms").between(1042, 1504))
    .select(
        "id",
        "service",
        "latency_ms",
        "key",
        "key_length",
        F.ceil("latency_ms").alias("latency_ms_whole"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.avg("key_length").over(w_part).alias("key_length_avg_2"),
        F.sum("latency_ms").over(w_part_2).alias("key_sum_latency_ms"),
    )
)

latest = (
    with_windows
    .withColumn("rn", F.rank().over(w3))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .select(
        "id",
        "latency_ms",
        "key",
        "key_length",
        "latency_ms_whole",
        "key_length_avg_2",
    )
    .orderBy(F.col("latency_ms_whole").desc(), F.col("key").desc())
)
