from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("session_id", "page").orderBy("event_time", "id")
w_part = Window.partitionBy("page")

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_time",
        F.lag("duration_sec").over(w_ordered).alias("prev_duration_sec"),
        F.lead("duration_sec").over(w_ordered).alias("next_duration_sec"),
        F.row_number().over(w_ordered).alias("seq_no"),
        F.min("duration_sec").over(w_part).alias("page_min_duration_sec"),
    )
)
