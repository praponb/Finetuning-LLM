from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("duration_sec", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id")

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "duration_sec",
        "event_time",
        F.row_number().over(w).alias("row_num"),
        F.lead("duration_sec", 2).over(w2).alias("next_duration_sec"),
    )
)
