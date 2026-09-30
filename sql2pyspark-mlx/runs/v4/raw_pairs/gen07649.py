from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_running = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.partitionBy("hour_window").orderBy(F.col("auth_status_code_avg_30m").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

metrics = (
    flattened
    .select(
        "*",
        F.avg("status_code").over(w).alias("status_code_avg_30m"),
        F.sum("status_code").over(w_running).alias("status_code_sum_2h"),
    )
)

base = (
    metrics
    .filter(F.col("log_ts") > F.lit("2025-06-01").cast("date"))
    .withColumn("attr_key_prefix", F.substring("attr_key", 1, 4))
    .withColumn("host_prefix", F.substring("host", 1, 4))
    .withColumn("attr_key_clean", F.coalesce("attr_key", F.lit("DIRECT")))
)

totals = (
    base
    .groupBy(
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
        F.when(F.col("status_code_avg_30m") >= 458, "HIGH").otherwise("LOW").alias("status_code_avg_30m_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max("log_ts").alias("last_log_ts"),
        F.count(F.when(F.col("host") == "h1", 1)).alias("h1_count"),
        F.min("log_ts").alias("started_at"),
        F.sum(F.when(F.col("service") == "auth", F.col("status_code_avg_30m")).otherwise(0)).alias("auth_status_code_avg_30m"),
        F.sort_array(F.collect_set("host")).alias("host_list"),
    )
    .filter(F.col("h1_count") > 1)
    .withColumn("auth_status_code_avg_30m_percent_rank", F.percent_rank().over(w3))
)

result = (
    totals
    .select(
        "hour_window",
        "status_code_avg_30m_tier",
        "cnt",
        "last_log_ts",
        "h1_count",
        "started_at",
        "host_list",
        "auth_status_code_avg_30m_percent_rank",
    )
    .orderBy(F.col("last_log_ts").desc())
)
