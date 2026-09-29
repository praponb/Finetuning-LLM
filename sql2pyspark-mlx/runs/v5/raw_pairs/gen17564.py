from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "key")
w2 = Window.partitionBy("key").orderBy("latency_ms", "id", "key")
w_part = Window.partitionBy("level").orderBy("status_code_share", "id", "key")
w4 = Window.partitionBy("level", "key").orderBy("latency_ms", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("key").orderBy("event_seq", "id", "key").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("level", "key").orderBy(F.col("latency_ms").desc(), "id", "key")

exploded = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

metrics = (
    exploded
    .select(
        "*",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

with_windows = (
    metrics
    .filter(F.col("latency_ms") > 1403)
    .select(
        "*",
        F.lead("latency_ms").over(w_part).alias("next_latency_ms"),
        F.lag("latency_ms").over(w_part).alias("prev_latency_ms"),
        F.lag("status_code_share").over(w_part).alias("prev_status_code_share"),
        F.row_number().over(w_part).alias("event_seq_2"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "level",
        "latency_ms",
        "key",
        "attr_value",
        "event_seq",
        "prev_latency_ms",
        "prev_status_code_share",
        "event_seq_2",
        F.max("latency_ms").over(w4).alias("running_max_latency_ms"),
        F.sum("latency_ms").over(w4).alias("running_sum_latency_ms"),
        F.min("latency_ms").over(w4).alias("running_min_latency_ms"),
        F.avg("prev_latency_ms").over(w5).alias("prev_latency_ms_avg_3"),
    )
)

latest = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "level",
        "latency_ms",
        "key",
        "attr_value",
        "event_seq_2",
        "running_min_latency_ms",
        "prev_latency_ms_avg_3",
    )
)

kept_keys = (
    latest
    .select("id", "level")
    .intersect(
        df_app_logs
        .filter(F.col("host") == "h1")
        .select("id", "level")
    )
)

verified = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "level",
        "latency_ms",
        "key",
        "event_seq_2",
        "running_min_latency_ms",
        F.when(F.col("key").rlike("^(region|trace)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("key_zone"),
    )
    .orderBy("key", "running_min_latency_ms")
)
