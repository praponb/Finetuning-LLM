from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("key").orderBy(F.col("log_ts").desc(), "id", "key")
w2 = Window.partitionBy("key").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("key").orderBy("log_ts", "id", "key")

flattened = (
    df_app_logs
    .select("id", "log_ts", "level", F.explode("context").alias("key", "value"))
)

metrics = (
    flattened
    .withColumn("row_seq", F.row_number().over(w_recent))
    .withColumn("last_known_key", F.last("key", ignorenulls=True).over(w2))
)

prev_events = metrics.withColumn("prev_log_ts", F.lag("log_ts").over(w3))

with_gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessions_app_logs = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

totals = (
    sessions_app_logs
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"), "island_id")
    .agg(
        F.count_if(F.col("key") == "region").alias("region_events"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.round(F.sum("row_seq") / F.nullif(F.count(F.when(F.col("key") == "trace", 1)), F.lit(0)), 2).alias("row_seq_per_trace"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq_2"),
        F.size(F.collect_set("key")).alias("num_keys"),
    )
)

result = (
    totals
    .select(
        "hour_window",
        "island_id",
        "key_list",
        "row_seq_per_trace",
        "avg_row_seq_2",
    )
    .orderBy(F.col("row_seq_per_trace").asc_nulls_last())
)
