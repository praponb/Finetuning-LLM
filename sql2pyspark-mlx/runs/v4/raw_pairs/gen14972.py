from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id")

result = (
    df_web_events
    .filter(F.col("event_time") >= "2025-02-01")
    .select(
        "id",
        "session_id",
        "duration_sec",
        F.sum("duration_sec").over(w).alias("session_id_sum_duration_sec"),
    )
)
