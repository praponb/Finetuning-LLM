from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type")
w2 = Window.partitionBy("session_id").orderBy("duration_sec", "id")

result = (
    df_web_events
    .select(
        "id",
        "event_type",
        "duration_sec",
        "session_id",
        F.max("duration_sec").over(w).alias("event_type_max_duration_sec"),
        F.ntile(10).over(w2).alias("bucket"),
    )
)
