from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") < "2024-11-01") & F.col("duration_sec").between(264, 308))
    .select(
        "id",
        "duration_sec",
        "page",
        "session_id",
        F.to_date("event_time").alias("event_time_date"),
    )
    .orderBy("session_id")
)
