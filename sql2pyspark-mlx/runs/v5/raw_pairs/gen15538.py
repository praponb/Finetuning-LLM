from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("event_type", "page").orderBy("duration_sec", "id")
w2 = Window.orderBy("event_time", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.orderBy(F.col("duration_sec").desc_nulls_first())
w4 = Window.partitionBy("event_type").orderBy("duration_sec", "id")

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "duration_sec",
        F.ntile(10).over(w_recent).alias("duration_sec_bucket"),
        F.avg("duration_sec").over(w2).alias("duration_sec_avg_5"),
        F.percent_rank().over(w3).alias("duration_sec_percent_rank"),
        F.lead("duration_sec", 2).over(w4).alias("next_duration_sec"),
    )
)
