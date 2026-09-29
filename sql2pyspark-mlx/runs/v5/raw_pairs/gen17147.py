from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("page", "session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("duration_sec", "id")
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.avg("duration_sec").alias("avg_web_event_duration_sec"))

metrics = (
    df_web_events
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec",
        F.lead("duration_sec").over(w_part).alias("next_duration_sec"),
        F.lag("duration_sec").over(w_part).alias("prev_duration_sec"),
        F.row_number().over(w_part).alias("seq_no"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

result = (
    metrics
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec",
        "next_duration_sec",
        "prev_duration_sec",
        "seq_no",
        "event_seq",
        F.col("avg_web_event_duration_sec"),
    )
)
