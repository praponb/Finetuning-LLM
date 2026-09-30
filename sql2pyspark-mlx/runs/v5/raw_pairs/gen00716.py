from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w2 = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc())
w3 = Window.partitionBy("event_type").orderBy("event_time", "id")

result = (
    df_web_events
    .filter(F.col("session_id") == 35)
    .select(
        "id",
        "event_type",
        "event_time",
        F.sum("duration_sec").over(w).alias("duration_sec_sum_30d"),
        F.dense_rank().over(w2).alias("duration_sec_dense_rank"),
        F.lead("duration_sec", 1).over(w3).alias("next_duration_sec"),
    )
)
