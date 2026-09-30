from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "duration_sec",
        "event_type",
        F.concat_ws("/", "event_type", "page").alias("event_type_page_key"),
    )
    .orderBy("event_type_page_key")
)
