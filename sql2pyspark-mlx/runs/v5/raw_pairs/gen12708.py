from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("event_time", "id")

result = (
    df_web_events
    .select(
        "id",
        "event_time",
        "duration_sec",
        F.lead("duration_sec", 1, 0).over(w).alias("next_duration_sec"),
    )
)
