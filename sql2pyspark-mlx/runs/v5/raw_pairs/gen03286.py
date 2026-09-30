from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("host", "level").orderBy("latency_ms", "id", "key").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("key").orderBy("log_ts", "id", "key")
w3 = Window.partitionBy("key").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

enriched = (
    lines
    .filter(F.col("host").isin(["h3", "h2"]))
    .withColumn("last3_max_status_code", F.max("status_code").over(w_running))
    .withColumn("last3_min_status_code", F.min("status_code").over(w_running))
    .withColumn("last3_avg_status_code", F.avg("status_code").over(w_running))
)

with_prev = enriched.withColumn("prev_log_ts", F.lag("log_ts").over(w2))

flagged = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "key",
        "v",
        "last3_max_status_code",
        "last3_min_status_code",
        F.sum("new_group_flag").over(w3).alias("session_id"),
    )
)

flagged_2 = (
    islands
    .withColumn("new_group_flag", F.when(F.col("log_ts") - F.lag("log_ts").over(w2) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

islands_app_logs = (
    flagged_2
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        "key",
        "v",
        "last3_max_status_code",
        "last3_min_status_code",
        "session_id",
        F.sum("new_group_flag").over(w3).alias("burst_id"),
    )
)

metrics = (
    islands_app_logs
    .select(
        "id",
        "log_ts",
        "status_code",
        "key",
        "v",
        "last3_max_status_code",
        "burst_id",
        F.count("last3_max_status_code").over(w3).alias("running_count_last3_max_status_code"),
        F.max("last3_max_status_code").over(w3).alias("running_max_last3_max_status_code"),
        F.avg("last3_max_status_code").over(w3).alias("running_avg_last3_max_status_code"),
    )
)

result = (
    metrics
    .select(
        "id",
        "key",
        "v",
        "burst_id",
        "running_count_last3_max_status_code",
        "running_max_last3_max_status_code",
        "running_avg_last3_max_status_code",
        F.when(F.col("key").rlike("^(version|trace)$"), "PRIORITY").when(F.col("key").rlike("^(region)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("key_zone"),
    )
    .orderBy(F.col("id").desc_nulls_last())
)
