from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id")
w3 = Window.partitionBy("page")
w4 = Window.partitionBy("session_id")

result = (
    df_web_events
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.lag("duration_sec", 1).over(w2).alias("prev_duration_sec"),
        F.stddev_samp("duration_sec").over(w3).alias("page_stddev_samp_duration_sec"),
        F.max("duration_sec").over(w4).alias("session_id_max_duration_sec"),
    )
)
