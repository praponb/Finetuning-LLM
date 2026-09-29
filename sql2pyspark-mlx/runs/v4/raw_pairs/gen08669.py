from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.orderBy(F.col("status_code").desc())
w3 = Window.partitionBy("service").orderBy("status_code")
w4 = Window.partitionBy("host").orderBy("latency_ms_scaled", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("latest_rank", "id", "k")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

enriched = (
    flattened
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4))
    .withColumn("status_code_cume_dist", F.cume_dist().over(w2))
)

valid = (
    enriched
    .filter(F.col("log_ts") >= F.lit("2025-06-01 15:00:00").cast("timestamp"))
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        "k",
        "status_code_cume_dist",
        F.round((F.col("latency_ms") * 0.85), 2).alias("latency_ms_scaled"),
        F.coalesce("k", F.lit("N/A")).alias("k_filled"),
    )
)

valid_app_logs = (
    valid
    .filter((F.col("log_ts") > F.lit("2025-06-02").cast("date")) & F.col("k").isNotNull())
    .select("*", F.datediff(F.current_date(), "log_ts").alias("days_since_log_ts"))
)

deduped = (
    valid_app_logs
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .select(
        "*",
        F.first("status_code_cume_dist", ignorenulls=True).over(w4).alias("first_known_status_code_cume_dist"),
        F.lag("days_since_log_ts", 2).over(w5).alias("prev_days_since_log_ts"),
    )
)
