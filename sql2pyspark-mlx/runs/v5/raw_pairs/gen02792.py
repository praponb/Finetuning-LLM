from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").asc_nulls_last())
w_hist = Window.partitionBy("event_type", "session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(-5, Window.currentRow)

with_windows = (
    df_web_events
    .select(
        "id",
        "page",
        "duration_sec",
        F.percent_rank().over(w).alias("duration_sec_percent_rank"),
        F.sum("duration_sec").over(w_hist).alias("running_duration_sec"),
        F.sum("duration_sec").over(w_recent).alias("running_duration_sec_2"),
        F.avg("duration_sec").over(w4).alias("duration_sec_avg_5"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "page",
        "duration_sec",
        "duration_sec_percent_rank",
        "running_duration_sec",
        "running_duration_sec_2",
    )
)
