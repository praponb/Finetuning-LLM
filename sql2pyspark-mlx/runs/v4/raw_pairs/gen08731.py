from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("page") == "/cart")
    .groupBy("session_id")
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", F.col("duration_sec")).otherwise(0)).alias("add_to_cart_duration_sec"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_summary.alias("SessionSummary"), F.col("sessions.id") == F.col("SessionSummary.session_id"), "left")
    .select(
        F.col("sessions.browser"),
        F.col("sessions.customer_id"),
        F.col("SessionSummary.avg_duration_sec"),
        F.col("SessionSummary.add_to_cart_duration_sec"),
    )
)
