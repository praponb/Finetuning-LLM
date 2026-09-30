from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("session_id").orderBy("event_time", "id")

result = (
    df_web_events
    .select(
        "id",
        "event_time",
        "duration_sec",
        F.lag("duration_sec").over(w_running).alias("prev_duration_sec"),
        F.lead("duration_sec").over(w_running).alias("next_duration_sec"),
        F.row_number().over(w_running).alias("event_seq"),
    )
)
