from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "inner")
    .filter((F.col("sessions.started_at") < "2023-04-01") & (F.col("web_events.event_type") == "add_to_cart"))
    .select(
        F.col("web_events.session_id"),
        F.col("web_events.event_type"),
        F.col("sessions.device"),
        F.col("sessions.browser"),
        F.col("sessions.country"),
        F.concat_ws("-", F.col("web_events.event_type"), F.col("web_events.page")).alias("event_type_page_key"),
    )
)
