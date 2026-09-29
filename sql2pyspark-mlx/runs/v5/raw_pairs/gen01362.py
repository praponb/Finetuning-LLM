from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("duration_sec", "id")
w2 = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(-2, -1)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.avg("duration_sec").alias("avg_web_event_duration_sec"))

with_windows = (
    df_web_events
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        F.lead("duration_sec", 1, 0).over(w).alias("next_duration_sec"),
        F.max("duration_sec").over(w2).alias("duration_sec_max_30d"),
        F.avg("duration_sec").over(w3).alias("duration_sec_avg_2_prior"),
    )
)

result = (
    with_windows
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec_max_30d",
        "duration_sec_avg_2_prior",
        F.col("avg_web_event_duration_sec"),
    )
    .orderBy(F.col("page").desc())
)
