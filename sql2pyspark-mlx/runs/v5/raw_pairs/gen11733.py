from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key")
w2 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.orderBy(F.col("latency_ms").desc_nulls_last())
w4 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w5 = Window.partitionBy("service", "host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w6 = Window.partitionBy("attr_key", "host")
w7 = Window.partitionBy("service", "host").orderBy("latency_ms_share")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

metrics = (
    flattened
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4))
    .withColumn("latency_ms_count_1h", F.count("latency_ms").over(w2))
    .withColumn("latency_ms_rank", F.rank().over(w3))
)

cleaned = (
    metrics
    .filter((F.col("log_ts") > F.to_date(F.lit("2025-06-01"))) & F.col("attr_key").isNotNull())
    .withColumn("attr_key_prefix", F.substring("attr_key", 1, 2))
    .withColumn("attr_key_filled", F.coalesce("attr_key", F.lit("DIRECT")))
)

with_windows = (
    cleaned
    .select(
        "*",
        F.avg("latency_ms_count_1h").over(w4).alias("latency_ms_count_1h_avg_30m"),
        F.avg("latency_ms_count_1h").over(w5).alias("latency_ms_count_1h_avg_30m_2"),
        F.count("latency_ms_count_1h").over(w6).alias("attr_key_count_latency_ms_count_1h"),
        F.percent_rank().over(w7).alias("latency_ms_share_percent_rank"),
    )
)

cleaned_app_logs = (
    with_windows
    .filter(F.col("latency_ms") <= 1595)
    .select("*", F.date_format("log_ts", "MMM yyyy").alias("log_ts_label"))
)

valid = (
    cleaned_app_logs
    .filter(F.col("log_ts") >= F.lit("2025-06-01").cast("date"))
    .withColumn("latency_ms_tier", F.when(F.col("latency_ms") >= 1996, "High").when(F.col("latency_ms") >= 410, "Medium").otherwise("Low"))
)

result = (
    valid
    .select(
        "host",
        F.col("latency_ms_share_percent_rank").alias("LATENCY_MS_SHARE_PERCENT_RANK_VAL"),
        F.col("latency_ms_share").alias("LATENCY_MS_SHARE_VAL"),
    )
    .unpivot(
        ids=["host"],
        values=["LATENCY_MS_SHARE_PERCENT_RANK_VAL", "LATENCY_MS_SHARE_VAL"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
)
