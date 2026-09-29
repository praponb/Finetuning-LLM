from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "page").orderBy(F.col("duration_sec").desc())
w2 = Window.orderBy("duration_sec", "id")
w3 = Window.partitionBy("session_id", "page").orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        F.rank().over(w).alias("duration_sec_rank"),
        F.ntile(4).over(w2).alias("duration_sec_bucket"),
        F.ntile(5).over(w3).alias("duration_sec_bucket_2"),
    )
)
