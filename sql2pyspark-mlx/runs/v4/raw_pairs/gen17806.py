from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.partitionBy("k").orderBy(F.col("status_code").desc())
w3 = Window.orderBy("log_ts", "id", "k")
w4 = Window.partitionBy("host", "k").orderBy("is_h1", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w6 = Window.partitionBy("host").orderBy(F.col("is_h1").desc())
w7 = Window.partitionBy("k").orderBy(F.col("status_code").desc(), "id", "k")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

scored = (
    flattened
    .select(
        "*",
        F.max("status_code").over(w).alias("status_code_max_30m"),
        F.cume_dist().over(w2).alias("status_code_cume_dist"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

filtered = (
    scored
    .filter(F.col("log_ts") >= F.to_date(F.lit("2025-06-01")))
    .select("*", F.round("status_code", 1).alias("status_code_rounded"))
)

base = (
    filtered
    .filter((F.col("log_ts") > F.to_date(F.lit("2025-06-01"))) & F.col("k").isNotNull())
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        "k",
        "attr_value",
        "status_code_max_30m",
        "status_code_cume_dist",
        "event_seq",
        F.when(F.col("host") == "h1", 1).otherwise(0).alias("is_h1"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.last("k", ignorenulls=True).over(w4).alias("last_known_k"),
        F.avg("status_code_max_30m").over(w5).alias("status_code_max_30m_avg_15m"),
        F.rank().over(w6).alias("is_h1_rank"),
        F.row_number().over(w7).alias("event_seq_2"),
    )
)

daily = (
    with_windows
    .groupBy(
        F.when(F.col("status_code") >= 443, "LARGE").otherwise("SMALL").alias("status_code_tier"),
        F.when(F.col("event_seq") >= 8, "HIGH").otherwise("LOW").alias("event_seq_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.count("*").alias("num_rows"),
        F.size(F.collect_set("k")).alias("num_ks"),
    )
)

result = (
    daily
    .select("status_code_tier", "event_seq_tier", "num_rows", "num_ks")
    .orderBy(F.col("num_rows").asc_nulls_first())
)
