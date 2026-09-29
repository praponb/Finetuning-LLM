from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("event_time").desc())

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        F.dense_rank().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") == 1)
    .orderBy("rnk")
)
