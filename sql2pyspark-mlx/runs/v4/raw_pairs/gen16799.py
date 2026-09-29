from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.count(F.when(F.col("page") == "/cart", 1)).alias("cart_count"),
        F.count("*").alias("web_event_count"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_summary.alias("session_summary"), F.col("sessions.id") == F.col("session_summary.session_id"), "left")
    .select(
        F.col("sessions.id"),
        F.col("sessions.browser"),
        F.col("sessions.customer_id"),
        F.col("session_summary.cart_count"),
        F.col("session_summary.web_event_count"),
    )
    .orderBy("browser")
)
