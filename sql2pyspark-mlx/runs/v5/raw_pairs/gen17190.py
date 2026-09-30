from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key")
w2 = Window.partitionBy("attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.orderBy("log_ts", "id", "attr_key")
w4 = Window.partitionBy("attr_key").orderBy("is_flagged", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("attr_key").orderBy(F.col("status_code_high").desc_nulls_last())
w6 = Window.partitionBy("attr_key").orderBy(F.col("is_flagged_sum_1h").desc())
w7 = Window.orderBy("status_code", "id", "attr_key")
w8 = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key")
w9 = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("attr_key").orderBy("island_id", "id", "attr_key").rowsBetween(-2, Window.currentRow)
w11 = Window.partitionBy("attr_key").orderBy(F.col("seq_no").asc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

parsed = (
    flattened
    .select(
        "id",
        "log_ts",
        "status_code",
        "attr_key",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
    )
)

scored = (
    parsed
    .withColumn("attr_key_min_is_flagged", F.min("is_flagged").over(w))
    .withColumn("is_flagged_sum_1h", F.sum("is_flagged").over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("last_known_is_flagged", F.last("is_flagged", ignorenulls=True).over(w4))
    .withColumn("status_code_high_dense_rank", F.dense_rank().over(w5))
)

metrics = (
    scored
    .select(
        "*",
        F.rank().over(w6).alias("is_flagged_sum_1h_rank"),
        F.sum("last_known_is_flagged").over(w4).alias("cumulative_last_known_is_flagged"),
        F.lag("attr_key_min_is_flagged", 1, 0).over(w7).alias("prev_attr_key_min_is_flagged"),
        F.last("last_known_is_flagged", ignorenulls=True).over(w4).alias("last_known_last_known_is_flagged"),
    )
)

filtered = (
    metrics
    .filter((F.col("log_ts") > F.lit("2025-06-01 16:00:00").cast("timestamp")) & F.col("attr_key").isNotNull())
    .select("*", F.round("status_code_high", 1).alias("status_code_high_rounded"))
)

prev_events = filtered.withColumn("prev_log_ts", F.lag("log_ts").over(w8))

gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "log_ts",
        "attr_key",
        "attr_value",
        "status_code_high",
        "is_flagged_sum_1h",
        "seq_no",
        "last_known_is_flagged",
        "status_code_high_dense_rank",
        "is_flagged_sum_1h_rank",
        "cumulative_last_known_is_flagged",
        "prev_attr_key_min_is_flagged",
        "status_code_high_rounded",
        F.sum("new_group_flag").over(w9).alias("island_id"),
    )
)

enriched = (
    islands
    .withColumn("last3_min_is_flagged_sum_1h_rank", F.min("is_flagged_sum_1h_rank").over(w10))
    .withColumn("last3_max_is_flagged_sum_1h_rank", F.max("is_flagged_sum_1h_rank").over(w10))
    .withColumn("last3_count_is_flagged_sum_1h_rank", F.count("is_flagged_sum_1h_rank").over(w10))
    .withColumn("seq_no_dense_rank", F.dense_rank().over(w11))
)

daily = (
    enriched
    .groupBy(F.date_trunc("hour", "log_ts").alias("hour_window"))
    .agg(
        F.max(F.when(F.col("attr_key") == "trace", F.col("last3_count_is_flagged_sum_1h_rank"))).alias("trace_max_last3_count_is_flagged_sum_1h_rank"),
        F.round(F.avg("last3_min_is_flagged_sum_1h_rank"), 2).alias("avg_last3_min_is_flagged_sum_1h_rank"),
        F.count("*").alias("num_rows"),
        F.count_if(F.col("attr_key") == "region").alias("region_events"),
        F.count_if(F.col("attr_key") == "version").alias("version_events"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
    )
)

result = (
    daily
    .select("hour_window", "region_events", "version_events", "span_minutes")
    .orderBy("span_minutes", "hour_window")
)
