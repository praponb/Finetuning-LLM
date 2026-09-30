from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("k").orderBy("is_new_session", "id", "k")
w4 = Window.partitionBy("k").orderBy(F.col("row_num").desc(), "id", "k")

flattened = (
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

parsed = (
    flattened
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        "k",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
    )
)

cleaned = (
    parsed
    .filter(F.col("log_ts") >= F.lit("2025-06-01 09:00:00").cast("timestamp"))
    .select(
        "id",
        "log_ts",
        "status_code",
        "k",
        "status_code_high",
        (F.col("has_fault") - F.col("status_code_high")).alias("has_fault_minus_status_code_high"),
        F.upper("k").alias("k_upper"),
    )
)

flagged = (
    cleaned
    .withColumn("is_new_session", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("is_new_session").over(w2))

ranked = (
    islands
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

filtered = (
    ranked
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & F.col("k").isNotNull())
    .select("*", F.round("status_code", 1).alias("status_code_rounded"))
)

ranked_app_logs = (
    filtered
    .withColumn("row_num_2", F.row_number().over(w4))
    .filter(F.col("row_num_2") == 1)
)

daily = (
    ranked_app_logs
    .groupBy("session_id")
    .agg(
        F.count(F.when(F.col("k") == "version", 1)).alias("version_count"),
        F.min("log_ts").alias("started_at"),
        F.round(F.avg("has_fault_minus_status_code_high"), 2).alias("avg_has_fault_minus_status_code_high"),
    )
)

result = (
    daily
    .select(
        "session_id",
        "version_count",
        "started_at",
        "avg_has_fault_minus_status_code_high",
    )
    .orderBy(F.col("version_count").desc_nulls_last())
)
