from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.max("duration_sec").alias("max_web_event_duration_sec"))

with_prev = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.sum("is_new_session").over(w2).alias("session_no"),
    )
)

result = (
    sessionized
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        "session_no",
        F.col("max_web_event_duration_sec"),
    )
    .orderBy(F.col("session_id").desc_nulls_first())
)
