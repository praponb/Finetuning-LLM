from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("host")
w3 = Window.partitionBy("week_start").orderBy(F.col("debug_events").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

cleaned = (
    exploded
    .filter((F.col("log_ts") >= F.lit("2025-06-01").cast("date")) & F.col("attr_key").isNotNull())
    .withColumn("level_title", F.initcap(F.trim("level")))
    .withColumn("host_upper", F.upper("host"))
)

filtered = (
    cleaned
    .filter((F.col("log_ts") >= F.lit("2025-06-01").cast("date")) & F.col("attr_key").isNotNull())
    .withColumn("host_group", F.when(F.col("host").isin(["h1", "h4"]), "Tier 1").otherwise("Other"))
)

with_windows = (
    filtered
    .filter(F.col("status_code") >= 379)
    .select(
        "*",
        F.corr("status_code", "latency_ms").over(w).alias("corr_status_code_latency_ms"),
        F.max("status_code").over(w2).alias("host_max_status_code"),
        F.max("status_code").over(w).alias("level_max_status_code"),
    )
)

filtered_app_logs = (
    with_windows
    .filter(F.col("log_ts") >= F.to_timestamp(F.lit("2025-06-01 15:00:00")))
    .withColumn("latency_ms_minus_status_code", (F.col("latency_ms") - F.col("status_code")))
    .withColumn("log_ts_day", F.col("log_ts").cast("date"))
    .withColumn("attr_key_filled", F.coalesce("attr_key", F.lit("N/A")))
)

grouped = (
    filtered_app_logs
    .groupBy(
        F.date_trunc("WEEK", "log_ts_day").alias("week_start"),
        F.when(F.col("corr_status_code_latency_ms") >= 0.33, "HIGH").otherwise("LOW").alias("corr_status_code_latency_ms_tier"),
        "level",
    )
    .agg(
        F.count_if(F.col("level") == "DEBUG").alias("debug_events"),
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles"),
    )
    .withColumn("debug_events_dense_rank", F.dense_rank().over(w3))
)

result = (
    grouped
    .select(
        "week_start",
        "corr_status_code_latency_ms_tier",
        "level",
        "debug_events",
        F.when(F.col("level").rlike("^(WARN|INFO)$"), "GROUP_B").when(F.col("level").rlike("^(ERROR|DEBUG)$"), "GROUP_A").otherwise("CORE").alias("level_zone"),
    )
    .orderBy(F.col("week_start").asc_nulls_first())
)
