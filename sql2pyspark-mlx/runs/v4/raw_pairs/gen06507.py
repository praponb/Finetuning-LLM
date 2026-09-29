from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(((F.col("session_id") == 28) & (F.to_date("event_time") >= "2025-03-01")) | (F.col("session_id") == 18))
    .select(
        "duration_sec",
        F.col("duration_sec").cast("string").alias("duration_sec_num"),
    )
)
