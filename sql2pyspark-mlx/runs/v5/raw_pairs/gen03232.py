from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("sessions.id") == F.col("web_events.session_id"), "inner")
    .filter((F.col("web_events.duration_sec") < 385) & (F.col("sessions.device") == "desktop"))
    .select(
        F.col("web_events.id"),
        F.col("web_events.duration_sec"),
        F.col("sessions.customer_id"),
        F.col("sessions.browser"),
        F.col("sessions.device"),
        F.when(F.col("web_events.event_type").isin(["add_to_cart", "click"]), "Core").otherwise("Other").alias("event_type_group"),
    )
)
