from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([7, 36]))
    .select("id", "session_id")
    .subtract(
        df_web_events
        .filter(F.col("duration_sec").between(127, 425))
        .select("id", "session_id")
    )
)
