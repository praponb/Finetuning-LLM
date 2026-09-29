from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        F.sum("duration_sec").over(w).alias("running_duration_sec"),
        F.round(F.avg("duration_sec").over(w2), 2).alias("duration_sec_moving_avg_4"),
    )
)
