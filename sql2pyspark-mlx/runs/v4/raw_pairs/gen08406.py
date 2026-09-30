from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "inner")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "right")
    .filter(F.col("sessions.country").isin(["UK", "US", "JP"]))
    .select(
        F.col("web_events.event_time"),
        F.col("web_events.page"),
        F.col("sessions.browser"),
        F.col("customers.name"),
        F.col("customers.city"),
    )
)
