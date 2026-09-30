from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("event_type").orderBy("event_time", "id")

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.lag("duration_sec").over(w_grp).alias("prev_duration_sec"),
        F.row_number().over(w_grp).alias("row_seq"),
    )
)
