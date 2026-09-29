from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("event_time", "id")

result = (
    df_web_events
    .filter(F.col("session_id").isin([25, 26, 29, 33]))
    .select(
        "id",
        "event_time",
        "duration_sec",
        F.lag("duration_sec", 1).over(w).alias("prev_duration_sec"),
    )
    .orderBy(F.col("prev_duration_sec").desc(), "id")
)
