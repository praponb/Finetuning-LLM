from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("duration_sec", "id")
w_hist = Window.partitionBy("page", "session_id").orderBy("event_time", "id").rowsBetween(-6, -1)

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        F.ntile(4).over(w_ordered).alias("duration_sec_bucket"),
        F.avg("duration_sec").over(w_hist).alias("duration_sec_avg_6_prior"),
    )
)
