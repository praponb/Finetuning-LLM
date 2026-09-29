from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.partitionBy("host", "service")
w3 = Window.partitionBy("host")
w4 = Window.partitionBy("service", "key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w5 = Window.partitionBy("level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w6 = Window.partitionBy("service")
w7 = Window.partitionBy("host").orderBy(F.col("status_code_avg_30m").desc())
w8 = Window.partitionBy("service").orderBy(F.col("latency_ms").asc_nulls_first())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

valid = (
    lines
    .filter(F.col("service").isin(["checkout", "billing"]))
    .withColumn("host_title", F.initcap(F.trim("host")))
    .withColumn("latency_ms_level", F.when(F.col("latency_ms") >= 2333, "High").when(F.col("latency_ms") >= 1141, "Medium").otherwise("Low"))
    .withColumn("resolved_key", F.coalesce("key", F.lit("OTHER")))
)

with_windows = (
    valid
    .filter(F.col("key") != "trace")
    .select(
        "*",
        F.avg("status_code").over(w).alias("status_code_avg_30m"),
        F.max("status_code").over(w2).alias("host_max_status_code"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4).alias("latency_ms_share"),
        F.count("status_code").over(w4).alias("status_code_count_1h"),
        F.avg("status_code").over(w5).alias("status_code_avg_30m_2"),
    )
)

filtered = (
    with_windows
    .filter(F.col("status_code").between(274, 412))
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "v",
        "host_title",
        "latency_ms_level",
        "resolved_key",
        "status_code_avg_30m",
        "host_max_status_code",
        "latency_ms_share",
        "status_code_count_1h",
        F.concat("level", F.lit("/"), "key").alias("level_key"),
        F.coalesce("key", F.lit("OTHER")).alias("key_clean"),
    )
)

with_windows_app_logs = (
    filtered
    .select(
        "service",
        "host",
        "log_ts",
        "v",
        "host_title",
        "host_max_status_code",
        "latency_ms_share",
        "status_code_count_1h",
        "level_key",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w6)), 4).alias("latency_ms_share_2"),
        F.dense_rank().over(w7).alias("status_code_avg_30m_dense_rank"),
        F.percent_rank().over(w8).alias("latency_ms_percent_rank"),
    )
)

daily = (
    with_windows_app_logs
    .groupBy(
        "service",
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
    )
    .agg(
        F.percentile_approx("latency_ms_share", [0.25, 0.5, 0.75]).alias("latency_ms_share_quartiles"),
        F.max("status_code_avg_30m_dense_rank").alias("max_status_code_avg_30m_dense_rank"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.count("*").alias("cnt"),
        F.percentile_approx("status_code_avg_30m_dense_rank", 0.9).alias("status_code_avg_30m_dense_rank_p90"),
        F.round(F.sum("latency_ms_share") / F.nullif(F.count(F.when(F.col("host") == "h2", 1)), F.lit(0)), 2).alias("latency_ms_share_per_h2"),
    )
)

result = (
    daily
    .select(
        "service",
        "hour_window",
        "activity_day",
        "latency_ms_share_quartiles",
        "max_status_code_avg_30m_dense_rank",
        "span_minutes",
        "cnt",
        "latency_ms_share_per_h2",
        F.when(F.col("service").rlike("^(auth|checkout)$"), "OTHER_ZONE").when(F.col("service").rlike("^(billing|search)$"), "PRIORITY").otherwise("GROUP_B").alias("service_zone"),
    )
    .orderBy(F.col("cnt").desc())
)
