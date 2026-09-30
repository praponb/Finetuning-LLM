from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "event_type").orderBy("event_time", "id")

result = (
    df_web_events
    .filter(F.col("event_type") == "click")
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        (F.col("duration_sec") - F.lag("duration_sec", 1).over(w)).alias("duration_sec_change"),
    )
)
