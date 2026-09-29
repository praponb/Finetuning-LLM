from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id")

result = (
    df_web_events
    .filter(F.col("page") != "/cart")
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        F.lead("duration_sec", 2).over(w).alias("next_duration_sec"),
        F.sum("duration_sec").over(w2).alias("running_duration_sec"),
    )
)
