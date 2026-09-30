from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key", "host").orderBy(F.col("log_ts").desc(), "id", "attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

deduped = (
    exploded
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "host", "attr_key", "attr_value")
)

valid = (
    deduped
    .filter(F.col("attr_key").isNotNull() & (F.col("attr_key").eqNullSafe(F.lit("version"))))
    .select(
        "*",
        F.substring("attr_key", 1, 4).alias("attr_key_prefix"),
        F.when(F.col("attr_key") == "trace", 1).otherwise(0).alias("is_trace"),
        F.coalesce("attr_key", F.lit("N/A")).alias("attr_key_filled"),
    )
)

base = (
    valid
    .filter(F.col("is_trace") <= 42)
    .select(
        "id",
        "host",
        "attr_key",
        "attr_value",
        "attr_key_prefix",
        "attr_key_filled",
        F.concat_ws("-", "attr_key", "host").alias("attr_key_host_key"),
    )
)

daily = (
    base
    .groupBy("attr_key", "host")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("attr_key") == "region", 1)).alias("region_count"),
        F.count_if(F.col("host") == "h4").alias("h4_events"),
    )
)

result = (
    daily
    .select(
        "attr_key",
        "host",
        "distinct_ids",
        "region_count",
        "h4_events",
        F.when(F.col("host").rlike("^(h2|h1)$"), "GROUP_A").otherwise("PRIORITY").alias("host_zone"),
    )
    .orderBy(F.col("host_zone").desc())
)
