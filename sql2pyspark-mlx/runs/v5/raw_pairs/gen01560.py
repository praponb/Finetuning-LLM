from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

with_gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_web_events = (
    with_gaps
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        F.sum("is_new_burst").over(w2).alias("island_id"),
    )
)

result = (
    sessions_web_events
    .groupBy(
        F.date_trunc("QUARTER", "event_time").alias("quarter_start"),
        F.when(F.col("island_id") >= 8, "LARGE").otherwise("SMALL").alias("island_id_tier"),
    )
    .agg(
        F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_view_count"),
        F.round(F.avg("island_id"), 2).alias("avg_island_id"),
    )
    .orderBy("page_view_count")
)
