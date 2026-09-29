from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_web_events
    .filter(F.col("duration_sec").between(143, 406))
    .select(
        "id",
        "page",
        "duration_sec",
        "session_id",
        "event_time",
        F.dense_rank().over(w).alias("duration_sec_dense_rank"),
        F.round(F.avg("duration_sec").over(w2), 2).alias("duration_sec_moving_avg_7"),
    )
)
