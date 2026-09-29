from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("key")
w_recent = Window.orderBy("latency_ms", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("status_code", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("key").orderBy("status_code", "id", "key")
w5 = Window.partitionBy("key").orderBy(F.col("key_max_status_code").desc(), "id", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

scored = (
    flattened
    .select(
        "*",
        F.min("status_code").over(w_grp).alias("key_min_status_code"),
        F.last("key", ignorenulls=True).over(w_recent).alias("last_known_key"),
        F.sum("status_code").over(w3).alias("cumulative_status_code"),
        F.ntile(10).over(w4).alias("status_code_bucket"),
        F.max("status_code").over(w_grp).alias("key_max_status_code"),
    )
)

deduped = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

valid = (
    deduped
    .filter(F.col("key").isNotNull())
    .select(
        "*",
        F.coalesce("key_max_status_code", F.lit(0)).alias("key_max_status_code_filled"),
        F.coalesce("key", F.lit("NONE")).alias("key_filled"),
    )
)

filtered = (
    valid
    .filter(F.col("cumulative_status_code") < 761)
    .select(
        "id",
        "status_code",
        "key",
        "attr_value",
        "key_min_status_code",
        "cumulative_status_code",
        "status_code_bucket",
        "key_max_status_code",
        "key_max_status_code_filled",
        "key_filled",
        F.length("key").alias("key_length"),
        F.coalesce("key", F.lit("NONE")).alias("key_clean"),
    )
)

cleaned = (
    filtered
    .filter(F.col("key").isNotNull())
    .select(
        "*",
        F.col("status_code").cast("double").alias("status_code_num"),
        F.coalesce("key", F.lit("NONE")).alias("key_clean_2"),
    )
)

daily = (
    cleaned
    .groupBy(
        F.when(F.col("key_max_status_code") >= 452, "HIGH").otherwise("LOW").alias("key_max_status_code_tier"),
        "key",
    )
    .agg(
        F.round(F.sum("cumulative_status_code") / F.nullif(F.count(F.when(F.col("key") == "trace", 1)), F.lit(0)), 2).alias("cumulative_status_code_per_trace"),
        F.sum("status_code_num").alias("total_status_code_num"),
        F.round(F.sum("cumulative_status_code") / F.nullif(F.count(F.when(F.col("key") == "version", 1)), F.lit(0)), 2).alias("cumulative_status_code_per_version"),
        F.percentile_approx("status_code_bucket", 0.5).alias("status_code_bucket_p50"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.count(F.when(F.col("key") == "region", 1)).alias("region_rows"),
    )
    .filter(F.col("status_code_bucket_p50") < 0)
)

result = (
    daily
    .select(
        "key_max_status_code_tier",
        "key",
        "cumulative_status_code_per_trace",
        "total_status_code_num",
        "cumulative_status_code_per_version",
        "status_code_bucket_p50",
        "region_rows",
    )
)
