from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("latency_ms").desc(), "id", "attr_key")
w2 = Window.partitionBy("attr_key", "host").orderBy("latency_ms")
w3 = Window.partitionBy("attr_key").orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("attr_key").orderBy("latency_ms", "id", "attr_key")
w5 = Window.partitionBy("host").orderBy(F.col("latency_ms_bucket").asc_nulls_last(), "id", "attr_key")
w6 = Window.partitionBy("attr_key").orderBy("cumulative_status_code", "id", "attr_key")
w7 = Window.orderBy("latency_ms_bucket")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

metrics = (
    lines
    .select(
        "*",
        F.ntile(3).over(w).alias("latency_ms_bucket"),
        F.rank().over(w2).alias("latency_ms_rank"),
        F.sum("status_code").over(w3).alias("cumulative_status_code"),
    )
)

deduped = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    deduped
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    latest
    .withColumn("next_latency_ms_rank", F.lead("latency_ms_rank", 1).over(w6))
    .withColumn("latency_ms_bucket_rank", F.rank().over(w7))
)

matched_keys = (
    with_windows
    .select("host", "id")
    .subtract(
        df_app_logs
        .filter(~F.col("service").isin(["checkout", "auth"]))
        .select("host", "id")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.host") == F.col("k.host")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept_keys = (
    reconciled
    .select("host", "id")
    .subtract(
        df_app_logs
        .filter(F.col("latency_ms").between(1197, 2184))
        .select("host", "id")
    )
)

verified = (
    reconciled
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.host") == F.col("k.host")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("attr_key")
    .pivot("host", ["h4", "h2"])
    .agg(F.max("latency_ms").alias("amount"), F.count(F.lit(1)).alias("n"))
)
