from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.min("duration_sec").alias("min_web_event_duration_sec"))

enriched = (
    df_web_events
    .select(
        "*",
        F.sum("duration_sec").over(w_hist).alias("running_sum_duration_sec"),
        F.max("duration_sec").over(w_hist).alias("running_max_duration_sec"),
        F.avg("duration_sec").over(w_hist).alias("running_avg_duration_sec"),
    )
)

result = (
    enriched
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_type",
        "duration_sec",
        "running_sum_duration_sec",
        "running_avg_duration_sec",
        F.col("min_web_event_duration_sec"),
    )
)
