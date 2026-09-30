from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.to_date("event_time") >= "2023-12-29")
    .select("id", "duration_sec", "session_id")
    .unionByName(
        df_web_events
        .filter(F.col("event_type") == "add_to_cart")
        .select("id", "duration_sec", "session_id")
    )
    .distinct()
)
