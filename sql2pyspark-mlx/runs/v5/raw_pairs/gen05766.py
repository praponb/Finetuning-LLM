from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("sessions.customer_id"), "left")
    .groupBy(F.col("sessions.device"))
    .agg(
        F.sort_array(F.collect_set(F.col("web_events.event_type"))).alias("event_type_list"),
    )
)
