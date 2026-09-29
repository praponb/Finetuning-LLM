from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_time",
        "page",
        "duration_sec",
        F.first("page").over(w).alias("first_page"),
        F.round(F.avg("duration_sec").over(w2), 2).alias("duration_sec_moving_avg_3"),
    )
    .orderBy("page")
)
