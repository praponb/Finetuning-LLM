from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy("latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("attr_key")
w3 = Window.partitionBy("attr_key").orderBy(F.col("running_sum_latency_ms").asc_nulls_last())
w4 = Window.partitionBy("attr_key").orderBy(F.col("running_min_latency_ms").desc_nulls_last())
w5 = Window.partitionBy("attr_key").orderBy("running_max_latency_ms", "id", "attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

parsed = (
    exploded
    .filter(F.col("attr_key").isin(["trace", "version", "region"]))
    .select(
        "id",
        "latency_ms",
        "status_code",
        "attr_key",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

scored = (
    parsed
    .select(
        "id",
        "latency_ms",
        "status_code",
        "attr_key",
        F.count("latency_ms").over(w).alias("running_count_latency_ms"),
        F.sum("latency_ms").over(w).alias("running_sum_latency_ms"),
        F.max("latency_ms").over(w).alias("running_max_latency_ms"),
        F.min("latency_ms").over(w).alias("running_min_latency_ms"),
        F.corr("latency_ms", "status_code").over(w2).alias("corr_latency_ms_status_code"),
    )
)

scored_app_logs = (
    scored
    .filter(F.col("running_max_latency_ms").between(1700, 2300))
    .select(
        "id",
        "attr_key",
        "running_count_latency_ms",
        "running_sum_latency_ms",
        "corr_latency_ms_status_code",
        F.min("status_code").over(w2).alias("attr_key_min_status_code"),
        F.cume_dist().over(w3).alias("running_sum_latency_ms_cume_dist"),
        F.count("latency_ms").over(w2).alias("attr_key_count_latency_ms"),
        F.cume_dist().over(w4).alias("running_min_latency_ms_cume_dist"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

valid = (
    scored_app_logs
    .filter(F.col("corr_latency_ms_status_code") <= -0.4)
    .withColumn("running_sum_latency_ms_plus_running_count_latency_ms", (F.col("running_sum_latency_ms") + F.col("running_count_latency_ms")))
    .withColumn("attr_key_clean", F.coalesce("attr_key", F.lit("OTHER")))
)

valid_app_logs = (
    valid
    .filter(F.col("attr_key_min_status_code") >= 359)
    .select(
        "*",
        F.ceil("running_sum_latency_ms").alias("running_sum_latency_ms_whole"),
        F.coalesce("attr_key", F.lit("N/A")).alias("attr_key_filled"),
    )
)

result = (
    valid_app_logs
    .groupBy("attr_key")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
        F.round(F.sum("running_sum_latency_ms_plus_running_count_latency_ms") / F.nullif(F.count(F.when(F.col("attr_key") == "trace", 1)), F.lit(0)), 2).alias("running_sum_latency_ms_plus_running_count_latency_ms_per_trace"),
    )
    .orderBy(F.col("num_attr_keys").desc())
)
