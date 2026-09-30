from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(-2, Window.currentRow)
w_part = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(-3, Window.currentRow)

with_windows = (
    df_web_events
    .select(
        "*",
        F.min("duration_sec").over(w_recent).alias("last3_min_duration_sec"),
        F.sum("duration_sec").over(w_recent).alias("last3_sum_duration_sec"),
        F.max("duration_sec").over(w_recent).alias("last3_max_duration_sec"),
        F.avg("duration_sec").over(w_recent).alias("last3_avg_duration_sec"),
        F.avg("duration_sec").over(w_part).alias("duration_sec_avg_3"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "last3_min_duration_sec",
        "last3_sum_duration_sec",
        "last3_max_duration_sec",
    )
)
