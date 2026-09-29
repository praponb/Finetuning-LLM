from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "session_id")
w2 = Window.partitionBy("event_type", "session_id").orderBy(F.col("duration_sec").asc_nulls_last())

result = (
    df_web_events
    .select(
        "id",
        "event_time",
        "duration_sec",
        F.avg("duration_sec").over(w).alias("page_avg_duration_sec"),
        F.dense_rank().over(w2).alias("duration_sec_dense_rank"),
    )
)
