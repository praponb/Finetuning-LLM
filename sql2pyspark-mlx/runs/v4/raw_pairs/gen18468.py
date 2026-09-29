from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")

result = (
    df_web_events
    .filter(F.col("event_time") < "2024-03-19")
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        F.lead("duration_sec", 1).over(w).alias("next_duration_sec"),
    )
)
