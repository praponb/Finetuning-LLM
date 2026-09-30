from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.orderBy("event_time", "id")

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        "page",
        F.lag("duration_sec", 1).over(w).alias("prev_duration_sec"),
        F.first("page").over(w2).alias("first_page"),
    )
)
