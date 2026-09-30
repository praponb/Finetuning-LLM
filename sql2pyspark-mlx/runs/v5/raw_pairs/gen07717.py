from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level").orderBy(F.col("log_ts").desc(), "id", "k")
w4 = Window.partitionBy("k").orderBy(F.col("latency_ms_bit_and").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("k", "attr_value"),
    )
)

with_gaps = (
    lines
    .withColumn("session_start_flag", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "k",
        F.sum("session_start_flag").over(w2).alias("session_no"),
    )
)

cleaned = (
    sessionized
    .filter((F.col("level") == "DEBUG") & (F.col("log_ts") > F.lit("2025-06-01 06:00:00").cast("timestamp")))
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "k",
        "session_no",
        (F.col("latency_ms") - F.col("session_no")).alias("latency_ms_minus_session_no"),
        F.when(F.col("host").isin(["h1", "h4"]), "Group A").otherwise("Other").alias("host_group"),
        F.coalesce("k", F.lit("DIRECT")).alias("k_filled"),
    )
)

deduped = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select("id", "level", "latency_ms", "k", "latency_ms_minus_session_no")
)

cleaned_app_logs = (
    deduped
    .filter(F.col("k").isNotNull() & (F.col("level") != "WARN"))
    .select(
        "id",
        "level",
        "latency_ms",
        "k",
        "latency_ms_minus_session_no",
        F.substring("level", 1, 2).alias("level_prefix"),
        F.ceil("latency_ms").alias("latency_ms_whole"),
        F.coalesce("k", F.lit("NONE")).alias("resolved_k"),
    )
)

result = (
    cleaned_app_logs
    .groupBy(
        "k",
        F.when(F.col("latency_ms_whole") >= 66, "LARGE").otherwise("SMALL").alias("latency_ms_whole_tier"),
    )
    .agg(
        F.round(F.sum("latency_ms_minus_session_no") / F.nullif(F.count(F.when(F.col("level") == "INFO", 1)), F.lit(0)), 2).alias("latency_ms_minus_session_no_per_info"),
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
    )
    .filter(F.col("latency_ms_minus_session_no_per_info") > 0)
    .withColumn("latency_ms_bit_and_rank", F.rank().over(w4))
    .orderBy("latency_ms_bit_and")
)
