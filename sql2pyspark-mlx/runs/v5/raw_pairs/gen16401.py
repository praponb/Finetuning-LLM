from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.avg("duration_sec").alias("avg_web_event_duration_sec"))

prev_events = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    islands
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "prev_event_time",
        "session_start_flag",
        F.when(F.col("event_type").rlike("^(purchase|search)$"), "EXTENDED").when(F.col("event_type").rlike("^(page_view|click)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("event_type_zone"),
        F.col("avg_web_event_duration_sec"),
    )
    .orderBy("session_id", "event_time")
)
