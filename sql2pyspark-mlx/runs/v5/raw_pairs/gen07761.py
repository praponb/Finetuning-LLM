from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("event_time").desc())

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_time",
        "event_type",
        F.dense_rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 2)
    .select("id", "session_id", "event_time", "event_type")
    .orderBy(F.col("session_id").desc(), "event_type")
)
