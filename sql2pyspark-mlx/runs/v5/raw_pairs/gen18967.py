from pyspark.sql import functions as F

result = (
    df_web_events
    .select("event_type", "session_id")
    .intersectAll(
        df_web_events
        .filter(F.col("duration_sec").between(122, 307))
        .select("event_type", "session_id")
    )
)
