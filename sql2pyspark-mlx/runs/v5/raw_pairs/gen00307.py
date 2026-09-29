from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_web_events
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_web_events = (
    flagged
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.sum("session_start_flag").over(w2).alias("session_id_2"),
    )
)

result = (
    sessions_web_events
    .groupBy(
        "page",
        F.when(F.col("duration_sec") >= 457, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
    )
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_view_rows"),
        F.count(F.when(F.col("event_type") == "click", 1)).alias("click_count"),
    )
    .orderBy(F.col("click_count").desc_nulls_last())
)
