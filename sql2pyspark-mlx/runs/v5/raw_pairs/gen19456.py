from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.orderBy("event_time", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .select(
        "id",
        "duration_sec",
        F.lag("duration_sec", 1).over(w).alias("prev_duration_sec"),
        F.avg("duration_sec").over(w2).alias("duration_sec_avg_3_prior"),
        F.ntile(4).over(w3).alias("duration_sec_bucket"),
        F.ntile(10).over(w3).alias("duration_sec_bucket_2"),
    )
)
