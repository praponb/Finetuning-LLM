from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["page_view", "click"]))
    .select(
        "id",
        "page",
        "event_type",
        "session_id",
        F.upper("page").alias("page_upper"),
    )
)
