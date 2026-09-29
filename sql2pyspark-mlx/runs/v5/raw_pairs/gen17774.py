from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_web_events
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_web_events = (
    flagged
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        F.sum("new_group_flag").over(w2).alias("burst_id"),
    )
)

result = (
    sessions_web_events
    .groupBy("burst_id")
    .agg(
        F.percentile_approx("burst_id", [0.25, 0.5, 0.75]).alias("burst_id_quartiles"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.round(F.avg("burst_id"), 2).alias("avg_burst_id"),
        F.count(F.when(F.col("page") == "/product", 1)).alias("product_count"),
        F.max("event_time").alias("last_seen"),
    )
    .orderBy(F.col("avg_duration_sec").desc_nulls_last())
)
