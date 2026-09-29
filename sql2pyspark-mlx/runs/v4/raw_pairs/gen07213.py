from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("log_ts", "id", "key")
w2 = Window.partitionBy("key").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("service").orderBy("days_since_log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("service").orderBy("status_code", "id", "key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

flagged = (
    exploded
    .withColumn("is_new_session", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("is_new_session").over(w2))

filtered = (
    islands
    .filter(F.col("log_ts") >= F.lit("2025-06-02 00:00:00").cast("timestamp"))
    .select(
        "id",
        "service",
        "level",
        "status_code",
        "key",
        "value",
        "is_new_session",
        "session_id",
        F.datediff(F.current_date(), "log_ts").alias("days_since_log_ts"),
        F.length("key").alias("key_length"),
        F.coalesce("key", F.lit("UNKNOWN")).alias("key_clean"),
    )
)

enriched = (
    filtered
    .withColumn("cumulative_is_new_session", F.sum("is_new_session").over(w3))
    .withColumn("status_code_bucket", F.ntile(10).over(w4))
)

result = (
    enriched
    .withColumn("key_length_tier", F.when(F.col("key_length") >= 60, "PREMIUM").otherwise("STANDARD"))
    .groupingSets([["key", "key_length_tier"], ["key"]], "key", "key_length_tier")
    .agg(
        F.countDistinct("level").alias("distinct_levels"),
        F.percentile_approx("status_code_bucket", 0.5).alias("status_code_bucket_p50"),
        F.sum(F.when(F.col("key") == "region", F.col("cumulative_is_new_session")).otherwise(0)).alias("region_cumulative_is_new_session"),
        F.countDistinct("key").alias("distinct_keys"),
        F.round(F.sum("cumulative_is_new_session") / F.nullif(F.count(F.when(F.col("key") == "trace", 1)), F.lit(0)), 2).alias("cumulative_is_new_session_per_trace"),
    )
    .filter(F.col("status_code_bucket_p50") < 0)
)
