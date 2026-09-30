from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("log_ts", "id", "key")
w2 = Window.partitionBy("service").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("key", "host").orderBy("session_id", "id", "key").rowsBetween(-6, Window.currentRow)

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
        F.explode("context").alias("key", "v"),
    )
)

unpacked = (
    lines
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "key",
        "v",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("is_flagged"),
    )
)

with_gaps = (
    unpacked
    .withColumn("new_group_flag", F.when(F.col("log_ts") - F.lag("log_ts").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

with_windows = (
    islands
    .withColumn("last7_avg_is_flagged", F.avg("is_flagged").over(w_grp))
    .withColumn("last7_count_is_flagged", F.count("is_flagged").over(w_grp))
    .withColumn("last7_sum_is_flagged", F.sum("is_flagged").over(w_grp))
)

result = (
    with_windows
    .select(
        "id",
        "service",
        "host",
        "key",
        "v",
        "is_flagged",
        "new_group_flag",
        "session_id",
        "last7_avg_is_flagged",
        F.when(F.col("key").rlike("^(version|region)$"), "CORE").otherwise("OTHER_ZONE").alias("key_zone"),
    )
    .orderBy(F.col("is_flagged").desc_nulls_first(), "key")
)
