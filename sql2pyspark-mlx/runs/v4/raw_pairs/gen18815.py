from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([4, 25, 31, 39]))
    .select(
        "session_id",
        "page",
        F.concat_ws("-", "event_type", "page").alias("event_type_page_key"),
    )
)
