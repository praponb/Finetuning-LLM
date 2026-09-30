from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("service")
w4 = Window.partitionBy("service").orderBy(F.col("log_ts").desc(), "id", "k")
w5 = Window.partitionBy("service").orderBy("log_ts", "id", "k")
w6 = Window.partitionBy("service").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

with_gaps = (
    exploded
    .withColumn("new_group_flag", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

scored = (
    sessionized
    .select(
        "*",
        F.sum("status_code").over(w2).alias("cumulative_status_code"),
        F.round((F.col("status_code") / F.sum("status_code").over(w3)), 4).alias("status_code_share"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

gaps = (
    scored
    .withColumn("is_new_session", F.when(F.col("log_ts") - F.lag("log_ts").over(w5) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "k",
        "new_group_flag",
        "session_id",
        "cumulative_status_code",
        F.sum("is_new_session").over(w6).alias("session_no"),
    )
)

result = (
    islands
    .cube(
        F.when(F.col("cumulative_status_code") >= 1608, "HIGH").otherwise("LOW").alias("cumulative_status_code_tier"),
        F.date_trunc("hour", "log_ts").alias("hour_window"),
        F.when(F.col("session_no") >= 8, "HIGH").otherwise("LOW").alias("session_no_tier"),
    )
    .agg(
        F.percentile_approx("session_no", 0.5).alias("session_no_p50"),
        F.sum(F.when(F.col("k") == "version", F.col("cumulative_status_code")).otherwise(0)).alias("version_cumulative_status_code"),
        F.round(F.sum("new_group_flag") / F.nullif(F.count(F.when(F.col("k") == "version", 1)), F.lit(0)), 2).alias("new_group_flag_per_version"),
        F.round(F.sum("session_id") / F.nullif(F.count(F.when(F.col("k") == "trace", 1)), F.lit(0)), 2).alias("session_id_per_trace"),
        F.sum(F.when(F.col("service") == "auth", F.col("session_id"))).alias("auth_total_session_id"),
        F.percentile_approx("session_id", 0.5).alias("session_id_p50"),
    )
    .orderBy(F.col("session_id_per_trace").desc_nulls_last())
)
