from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key", "level").orderBy(F.col("latency_ms").desc(), "id", "attr_key")
w2 = Window.partitionBy("attr_key")
w3 = Window.partitionBy("host").orderBy("latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("service").orderBy("latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("level", "attr_key").orderBy("event_seq", "id", "attr_key")
w6 = Window.orderBy("event_seq", "id", "attr_key")
w7 = Window.partitionBy("host")
w8 = Window.partitionBy("service", "attr_key").orderBy("last_known_latency_ms", "id", "attr_key")
w9 = Window.orderBy(F.col("latency_ms").desc(), "id", "attr_key")
w10 = Window.partitionBy("service", "level").orderBy(F.col("attr_key_stddev_samp_latency_ms").desc(), "id", "attr_key")
w11 = Window.partitionBy("attr_key").orderBy(F.col("last_known_latency_ms_2").desc(), "id", "attr_key")
w12 = Window.orderBy("host_min_latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.partitionBy("service").orderBy(F.col("attr_key_stddev_samp_latency_ms").desc(), "id", "attr_key")
w14 = Window.partitionBy("service")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

cleaned = (
    flattened
    .filter(F.col("latency_ms") > 1857)
    .select(
        "*",
        F.when(F.col("attr_key").isin(["trace", "version"]), "Core").otherwise("Other").alias("attr_key_group"),
        F.coalesce("attr_key", F.lit("DIRECT")).alias("attr_key_filled"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "attr_key",
        "attr_key_group",
        "attr_key_filled",
        F.row_number().over(w).alias("event_seq"),
        F.stddev_samp("latency_ms").over(w2).alias("attr_key_stddev_samp_latency_ms"),
        F.last("latency_ms", ignorenulls=True).over(w3).alias("last_known_latency_ms"),
        F.last("latency_ms", ignorenulls=True).over(w4).alias("last_known_latency_ms_2"),
    )
)

enriched = (
    scored
    .withColumn("seq_no", F.row_number().over(w5))
    .withColumn("event_seq_bucket", F.ntile(3).over(w6))
    .withColumn("host_min_latency_ms", F.min("latency_ms").over(w7))
    .withColumn("last_known_latency_ms_bucket", F.ntile(10).over(w8))
    .withColumn("row_seq", F.row_number().over(w9))
)

latest = (
    enriched
    .withColumn("row_num", F.row_number().over(w10))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

ranked = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w11))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    ranked
    .select(
        "*",
        F.sum("last_known_latency_ms_bucket").over(w12).alias("running_last_known_latency_ms_bucket"),
        F.row_number().over(w9).alias("seq_no_2"),
        F.ntile(10).over(w13).alias("attr_key_stddev_samp_latency_ms_bucket"),
        F.min("latency_ms").over(w14).alias("service_min_latency_ms"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "attr_key",
        "attr_key_filled",
        "last_known_latency_ms",
        "seq_no",
        "host_min_latency_ms",
        F.when(F.col("level").rlike("^(WARN|ERROR)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("level_zone"),
    )
    .orderBy(F.col("level").desc())
)
