from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("event_type", "page").orderBy("event_time", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_web_events
    .select(
        "id",
        "event_type",
        F.avg("duration_sec").over(w_part).alias("last7_avg_duration_sec"),
        F.min("duration_sec").over(w_part).alias("last7_min_duration_sec"),
        F.max("duration_sec").over(w_part).alias("last7_max_duration_sec"),
        F.sum("duration_sec").over(w_part).alias("last7_sum_duration_sec"),
    )
)
