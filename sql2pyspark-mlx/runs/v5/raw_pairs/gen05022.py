from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("duration_sec")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id")

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "duration_sec",
        "event_type",
        "event_time",
        "page",
        F.dense_rank().over(w).alias("duration_sec_dense_rank"),
        F.first("page").over(w2).alias("first_page"),
    )
)
