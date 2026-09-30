from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("key").orderBy("status_code")
w_recent = Window.orderBy(F.col("status_code").desc())
w3 = Window.partitionBy("key")
w4 = Window.partitionBy("key").orderBy(F.col("adjusted_status_code").asc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

unpacked = (
    flattened
    .filter(F.col("status_code").between(264, 291))
    .select(
        "log_ts",
        "status_code",
        "key",
        "value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
    )
)

cleaned = (
    unpacked
    .filter((F.col("log_ts") > F.lit("2025-06-01 15:00:00").cast("timestamp")) & F.col("has_fault").between(0, 1))
    .select(
        "status_code",
        "key",
        "value",
        "status_code_high",
        F.round((F.col("status_code") * 0.85), 2).alias("adjusted_status_code"),
    )
)

scored = (
    cleaned
    .select(
        "key",
        "status_code_high",
        "adjusted_status_code",
        F.cume_dist().over(w_part).alias("status_code_cume_dist"),
        F.percent_rank().over(w_recent).alias("status_code_percent_rank"),
        F.round((F.col("status_code") / F.sum("status_code").over(w3)), 4).alias("status_code_share"),
    )
)

scored_app_logs = (
    scored
    .select(
        "key",
        "status_code_high",
        "status_code_cume_dist",
        "status_code_percent_rank",
        F.percent_rank().over(w4).alias("adjusted_status_code_percent_rank"),
        F.min("status_code_cume_dist").over(w3).alias("key_min_status_code_cume_dist"),
    )
)

cleaned_app_logs = (
    scored_app_logs
    .filter(~F.col("key").eqNullSafe(F.lit("trace")))
    .select(
        "key",
        "status_code_high",
        "status_code_cume_dist",
        "adjusted_status_code_percent_rank",
        F.when(F.col("key").isin(["region", "trace"]), "Tier 1").otherwise("Other").alias("key_group"),
        F.coalesce("key", F.lit("N/A")).alias("resolved_key"),
    )
)

cleaned_app_logs_2 = (
    cleaned_app_logs
    .filter(F.col("adjusted_status_code_percent_rank") < 0.46)
    .select(
        "*",
        (F.col("status_code_cume_dist") - F.col("adjusted_status_code_percent_rank")).alias("status_code_cume_dist_minus_adjusted_status_code_percent_rank"),
    )
)

summary = (
    cleaned_app_logs_2
    .groupBy(
        F.when(F.col("status_code_cume_dist") >= 0.88, "LARGE").otherwise("SMALL").alias("status_code_cume_dist_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.countDistinct("key").alias("distinct_keys"),
        F.count(F.when(F.col("key") == "region", 1)).alias("region_count"),
        F.countDistinct("key").alias("distinct_keys_2"),
        F.count_if(F.col("key") == "trace").alias("trace_events"),
        F.countDistinct("key").alias("distinct_keys_3"),
    )
)

result = (
    summary
    .select(
        "status_code_cume_dist_tier",
        "key_list",
        "distinct_keys",
        "distinct_keys_2",
        "trace_events",
    )
    .orderBy("distinct_keys")
)
