from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("session_id", "event_type").orderBy("event_time", "id")

result = (
    df_web_events
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        "session_id",
        F.round(F.avg("duration_sec").over(w), 2).alias("duration_sec_moving_avg_4"),
        F.row_number().over(w2).alias("rn"),
    )
)
