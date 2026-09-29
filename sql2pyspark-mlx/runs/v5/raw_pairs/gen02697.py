from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("k").orderBy("has_fault", "id", "k")
w4 = Window.partitionBy("k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w5 = Window.orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("k").orderBy("status_code_high", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("k").orderBy("has_fault", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("k").orderBy(F.col("next_status_code_high").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

unpacked = (
    exploded
    .select(
        "id",
        "log_ts",
        "status_code",
        "k",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

with_gaps = (
    unpacked
    .withColumn("new_group_flag", F.when(F.col("log_ts") - F.lag("log_ts").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "log_ts",
        "k",
        "has_fault",
        "status_code_high",
        F.sum("new_group_flag").over(w2).alias("session_id"),
    )
)

metrics = (
    islands
    .select(
        "*",
        F.lead("status_code_high", 1).over(w3).alias("next_status_code_high"),
        F.max("has_fault").over(w4).alias("has_fault_max_15m"),
        F.last("has_fault", ignorenulls=True).over(w5).alias("last_known_has_fault"),
        F.sum("has_fault").over(w6).alias("running_has_fault"),
        F.last("status_code_high", ignorenulls=True).over(w7).alias("last_known_status_code_high"),
    )
)

scored = (
    metrics
    .withColumn("prev_running_has_fault", F.lag("running_has_fault").over(w3))
    .withColumn("prev_last_known_has_fault", F.lag("last_known_has_fault").over(w3))
    .withColumn("next_last_known_has_fault", F.lead("last_known_has_fault").over(w3))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("next_status_code_high_percent_rank", F.percent_rank().over(w8))
)

matched_keys = (
    scored
    .select("id")
    .intersect(
        df_app_logs
        .filter(~F.col("host").isin(["h2", "h1"]))
        .select("id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
        F.when(F.col("next_status_code_high") >= 2, "PREMIUM").otherwise("STANDARD").alias("next_status_code_high_tier"),
    )
    .agg(
        F.count(F.when(F.col("k") == "trace", 1)).alias("trace_count"),
        F.round(F.sum("has_fault_max_15m") / F.nullif(F.count(F.when(F.col("k") == "trace", 1)), F.lit(0)), 2).alias("has_fault_max_15m_per_trace"),
        F.avg("last_known_has_fault").alias("avg_last_known_has_fault"),
    )
    .orderBy("trace_count")
)
