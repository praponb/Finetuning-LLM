from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device").orderBy("event_time", "id")
w4 = Window.partitionBy("device").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_web_events
    .filter(F.col("event_time") > F.to_date(F.lit("2023-08-13")))
    .select("*", F.col("event_time").cast("date").alias("event_time_day"))
)

enriched = (
    cleaned
    .alias("e")
    .join(df_sessions.alias("b"), F.col("e.session_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.country"), F.col("b.browser"), F.col("b.device"))
)

gaps = (
    enriched
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_web_events = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

with_prev = sessions_web_events.withColumn("prev_event_time", F.lag("event_time").over(w3))

flagged = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_web_events_2 = flagged.withColumn("island_id", F.sum("session_start_flag").over(w4))

result = (
    sessions_web_events_2
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        "event_time_day",
        "browser",
        "is_new_burst",
        "burst_id",
    )
    .orderBy(F.col("event_type").desc())
)
