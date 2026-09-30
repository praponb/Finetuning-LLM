from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.filter(F.col("customer_id").isNotNull()).alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "left_semi")
    .filter(F.col("web_events.page").isin(["/product", "/home", "/search"]))
    .select(
        F.col("web_events.id"),
        F.col("web_events.page"),
        F.col("web_events.event_type"),
        F.col("web_events.duration_sec"),
    )
)
