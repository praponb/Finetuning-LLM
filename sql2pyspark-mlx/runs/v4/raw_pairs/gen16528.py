from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("event_time").desc(), "id")
w2 = Window.partitionBy("event_type").orderBy("event_time")

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_time",
        "event_type",
        F.first("event_type").over(w).alias("first_event_type"),
        F.percent_rank().over(w2).alias("event_time_pct_rank"),
    )
    .orderBy(F.col("session_id").desc(), "first_event_type")
)
