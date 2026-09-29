from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("log_ts_day", "id", "key")
w2 = Window.partitionBy("key").orderBy("log_ts_day_period", "id", "key")
w3 = Window.partitionBy("key").orderBy("log_ts_day_period", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("key").orderBy("prev_log_ts_day_period", "id", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

cleaned = (
    flattened
    .filter(F.col("log_ts") >= F.lit("2025-06-01").cast("date"))
    .select(
        "id",
        "key",
        "value",
        F.col("log_ts").cast("date").alias("log_ts_day"),
        F.coalesce("key", F.lit("UNKNOWN")).alias("resolved_key"),
    )
)

cleaned_app_logs = (
    cleaned
    .filter(F.col("key").eqNullSafe(F.lit("region")))
    .select(
        "id",
        "key",
        "value",
        "log_ts_day",
        F.date_trunc("WEEK", "log_ts_day").alias("log_ts_day_period"),
    )
)

ranked = (
    cleaned_app_logs
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "key", "log_ts_day_period")
)

prev_events = ranked.withColumn("prev_log_ts_day_period", F.lag("log_ts_day_period").over(w2))

gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_log_ts_day_period").isNull() | (F.col("log_ts_day_period") > F.col("prev_log_ts_day_period") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("new_group_flag").over(w3))

result = (
    islands
    .withColumn("next_new_group_flag", F.lead("new_group_flag").over(w_part))
    .withColumn("prev_new_group_flag", F.lag("new_group_flag").over(w_part))
    .withColumn("next_session_no", F.lead("session_no").over(w_part))
    .withColumn("seq_no", F.row_number().over(w_part))
)
