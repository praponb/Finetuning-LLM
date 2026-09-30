from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key")
w2 = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("service").orderBy("log_ts", "id", "attr_key")
w4 = Window.partitionBy("attr_key").orderBy(F.col("session_no").desc(), "id", "attr_key")
w5 = Window.partitionBy("service").orderBy("prev_log_ts", "id", "attr_key")
w6 = Window.partitionBy("service").orderBy("prev_log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

prev_events = lines.withColumn("prev_log_ts", F.lag("log_ts").over(w))

gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

deduped = (
    islands
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

latest = (
    deduped
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

gaps_2 = (
    latest
    .withColumn("is_new_burst", F.when(F.col("prev_log_ts") - F.lag("prev_log_ts").over(w5) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands_app_logs = gaps_2.withColumn("island_id", F.sum("is_new_burst").over(w6))

result = (
    islands_app_logs
    .select(
        "id",
        "service",
        "attr_key",
        "attr_value",
        "prev_log_ts",
        "is_new_session",
        "session_no",
        "row_num",
        "island_id",
    )
    .orderBy(F.col("is_new_session").desc_nulls_last())
)
