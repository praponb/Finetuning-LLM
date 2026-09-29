from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("status_code", "id", "key")
w2 = Window.partitionBy("key")
w3 = Window.partitionBy("key", "level").orderBy(F.col("next_status_code").desc(), "id", "key")
w4 = Window.partitionBy("key").orderBy(F.col("event_seq").asc_nulls_first())
w5 = Window.partitionBy("key").orderBy(F.col("event_seq").desc(), "id", "key")

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

with_windows = (
    exploded
    .select(
        "id",
        "level",
        "key",
        F.lag("status_code").over(w).alias("prev_status_code"),
        F.lead("status_code").over(w).alias("next_status_code"),
        F.lead("latency_ms").over(w).alias("next_latency_ms"),
        F.row_number().over(w).alias("event_seq"),
        F.min("status_code").over(w2).alias("key_min_status_code"),
    )
)

ranked = (
    with_windows
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("id", "key", "prev_status_code", "event_seq", "key_min_status_code")
)

cleaned = (
    ranked
    .filter(F.col("prev_status_code") <= 277)
    .select(
        "id",
        "key",
        "prev_status_code",
        "event_seq",
        "key_min_status_code",
        F.substring("key", 1, 3).alias("key_prefix"),
    )
)

filtered = (
    cleaned
    .filter(F.col("key").isNotNull() & (~F.col("key").eqNullSafe(F.lit("trace"))))
    .select(
        "id",
        "key",
        "event_seq",
        "key_min_status_code",
        "key_prefix",
        F.upper("key").alias("key_upper"),
        F.when(F.col("key") == "version", 1).otherwise(0).alias("is_version"),
        F.coalesce("key", F.lit("UNKNOWN")).alias("key_filled"),
    )
)

deduped = (
    filtered
    .withColumn("dedup_rank", F.rank().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "key", "event_seq", "key_prefix", "key_filled")
)

deduped_app_logs = (
    deduped
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

summary = (
    deduped_app_logs
    .groupBy(
        F.when(F.col("event_seq") >= 8, "PREMIUM").otherwise("STANDARD").alias("event_seq_tier"),
    )
    .agg(
        F.countDistinct("key").alias("distinct_keys"),
        F.round(F.sum("event_seq") / F.nullif(F.count(F.when(F.col("key") == "region", 1)), F.lit(0)), 2).alias("event_seq_per_region"),
        F.count_if(F.col("key") == "region").alias("region_events"),
    )
)

result = summary.select("event_seq_tier", "region_events")
