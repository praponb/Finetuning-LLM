from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("host").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("host", "k").orderBy("log_ts", "id", "k").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("k").orderBy(F.col("session_no").desc(), "id", "k")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

prev_events = lines.withColumn("prev_log_ts", F.lag("log_ts").over(w))

gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

base = (
    islands
    .filter(F.col("k").isNotNull() & (F.col("log_ts") > F.to_date(F.lit("2025-06-01"))))
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        "k",
        "attr_value",
        "prev_log_ts",
        "is_new_session",
        "session_no",
        (F.col("is_new_session") - F.col("status_code")).alias("is_new_session_minus_status_code"),
        F.least("status_code", "is_new_session").alias("least_status_code_is_new_session"),
        F.coalesce("k", F.lit("N/A")).alias("resolved_k"),
    )
)

gaps_2 = (
    base
    .withColumn("is_new_burst", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessions_app_logs = (
    gaps_2
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "k",
        "attr_value",
        "prev_log_ts",
        "session_no",
        "least_status_code_is_new_session",
        "resolved_k",
        F.sum("is_new_burst").over(w2).alias("session_id"),
    )
)

enriched = (
    sessions_app_logs
    .select(
        "id",
        "host",
        "level",
        "k",
        "attr_value",
        "resolved_k",
        "session_id",
        F.count("least_status_code_is_new_session").over(w_recent).alias("last7_count_least_status_code_is_new_session"),
        F.max("least_status_code_is_new_session").over(w_recent).alias("last7_max_least_status_code_is_new_session"),
        F.sum("least_status_code_is_new_session").over(w_recent).alias("last7_sum_least_status_code_is_new_session"),
        F.ntile(3).over(w4).alias("session_no_bucket"),
    )
)

result = (
    enriched
    .cube(
        F.when(F.col("last7_max_least_status_code_is_new_session") >= 86, "PREMIUM").otherwise("STANDARD").alias("last7_max_least_status_code_is_new_session_tier"),
        F.when(F.col("last7_sum_least_status_code_is_new_session") >= 430, "LARGE").otherwise("SMALL").alias("last7_sum_least_status_code_is_new_session_tier"),
    )
    .agg(
        F.size(F.collect_set("level")).alias("num_levels"),
        F.count(F.when(F.col("k") == "version", 1)).alias("version_rows"),
        F.count(F.when(F.col("level") == "ERROR", 1)).alias("error_rows"),
        F.count(F.when(F.col("host") == "h1", 1)).alias("h1_rows"),
    )
    .orderBy(F.col("version_rows").desc())
)
