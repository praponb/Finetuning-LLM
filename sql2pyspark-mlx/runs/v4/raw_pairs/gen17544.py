from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("host")
w2 = Window.partitionBy("host").orderBy(F.col("latency_ms").desc_nulls_first())
w3 = Window.partitionBy("k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

scored = (
    exploded
    .withColumn("host_count_latency_ms", F.count("latency_ms").over(w_ordered))
    .withColumn("latency_ms_cume_dist", F.cume_dist().over(w2))
    .withColumn("latency_ms_avg_30m", F.avg("latency_ms").over(w3))
)

filtered = (
    scored
    .filter(F.col("log_ts") > F.lit("2025-06-01 14:00:00").cast("timestamp"))
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "host_count_latency_ms",
        "latency_ms_cume_dist",
        "latency_ms_avg_30m",
        F.length("level").alias("level_length"),
    )
)

filtered_app_logs = (
    filtered
    .filter(F.col("log_ts") >= F.lit("2025-06-01").cast("date"))
    .select("*", F.datediff(F.current_date(), "log_ts").alias("days_since_log_ts"))
)

cleaned = (
    filtered_app_logs
    .filter(F.col("log_ts") > F.to_timestamp(F.lit("2025-06-01 10:00:00")))
    .withColumn("greatest_status_code_days_since_log_ts", F.greatest("status_code", "days_since_log_ts"))
)

result = (
    cleaned
    .select(
        "service",
        "latency_ms",
        "status_code",
        "latency_ms_cume_dist",
        "latency_ms_avg_30m",
        "level_length",
        "days_since_log_ts",
        F.when(F.col("service").rlike("^(billing|auth)$"), "CORE").when(F.col("service").rlike("^(search|checkout)$"), "GROUP_A").otherwise("EXTENDED").alias("service_zone"),
    )
    .orderBy("status_code")
)
