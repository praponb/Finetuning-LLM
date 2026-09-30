from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id", "page")

result = (
    df_web_events
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        "session_id",
        "page",
        F.sum("duration_sec").over(w).alias("running_duration_sec"),
        F.min("duration_sec").over(w2).alias("session_id_min_duration_sec"),
    )
)
