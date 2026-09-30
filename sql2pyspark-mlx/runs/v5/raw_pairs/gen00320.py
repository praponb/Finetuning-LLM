from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy("status_code_high", "id", "attr_key")
w2 = Window.partitionBy("attr_key")
w3 = Window.partitionBy("attr_key").orderBy("status_code_high", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("attr_key").orderBy(F.col("status_code").desc(), "id", "attr_key")
w5 = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key")
w6 = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

parsed = (
    flattened
    .select(
        "id",
        "log_ts",
        "status_code",
        "attr_key",
        "value",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

scored = (
    parsed
    .withColumn("status_code_high_bucket", F.ntile(10).over(w))
    .withColumn("status_code_share", F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4))
    .withColumn("running_status_code", F.sum("status_code").over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
)

prev_events = scored.withColumn("prev_log_ts", F.lag("log_ts").over(w5))

gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w6))

scored_app_logs = (
    sessionized
    .select(
        "id",
        "log_ts",
        "status_code",
        "attr_key",
        "is_flagged",
        "status_code_high",
        "status_code_high_bucket",
        "status_code_share",
        "running_status_code",
        "seq_no",
        "prev_log_ts",
        "is_new_burst",
        "burst_id",
        F.sum("status_code").over(w7).alias("status_code_sum_15m"),
        F.stddev_samp("is_flagged").over(w2).alias("attr_key_stddev_samp_is_flagged"),
    )
)

totals = (
    scored_app_logs
    .groupBy(
        F.when(F.col("status_code_sum_15m") >= 1799, "HIGH").otherwise("LOW").alias("status_code_sum_15m_tier"),
        "seq_no",
    )
    .agg(
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
        F.count("*").alias("cnt"),
        F.percentile_approx("is_flagged", [0.25, 0.5, 0.75]).alias("is_flagged_quartiles"),
        F.count(F.when(F.col("attr_key") == "version", 1)).alias("version_count"),
        F.count_if(F.col("attr_key") == "version").alias("version_events"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
    )
    .filter(F.col("version_events") >= 1)
)

result = (
    totals
    .select(
        "status_code_sum_15m_tier",
        "seq_no",
        "attr_key_list",
        "cnt",
        "is_flagged_quartiles",
    )
    .orderBy("seq_no")
)
