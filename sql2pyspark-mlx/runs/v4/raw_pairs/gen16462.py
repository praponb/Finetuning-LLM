from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(9, 12) & (F.col("event_time") < "2023-10-01"))
    .select(
        "id",
        "session_id",
        F.concat("page", F.lit("/"), "event_type").alias("page_event_type"),
    )
)
