from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 40)
    .select("id", "event_type")
    .subtract(
        df_web_events
        .filter(F.col("session_id").isin([2, 21, 24]))
        .select("id", "event_type")
    )
)
