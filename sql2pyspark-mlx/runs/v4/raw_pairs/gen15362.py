from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc())

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "duration_sec",
        "event_time",
        F.dense_rank().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 3)
    .select("id", "session_id", "duration_sec", "event_time")
    .orderBy("event_time")
)
