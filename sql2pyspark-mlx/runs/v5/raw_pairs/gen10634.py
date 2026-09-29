from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("sessions.id") == F.col("web_events.session_id"), "left")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("sessions.customer_id"), "inner")
    .filter(F.col("customers.is_active") & ~F.col("sessions.device").isin(["tablet", "desktop"]))
    .select(
        F.col("web_events.id"),
        F.col("web_events.event_time"),
        F.col("web_events.session_id"),
        F.col("sessions.device"),
        F.col("sessions.browser"),
        F.col("customers.email"),
    )
    .orderBy("device")
)
