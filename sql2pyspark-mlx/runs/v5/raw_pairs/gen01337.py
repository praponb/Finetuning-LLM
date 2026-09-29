from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.countDistinct("duration_sec").alias("unique_pages"),
        F.count(F.when(F.col("page") == "/cart", 1)).alias("cart_count"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_stats.alias("session_stats"), F.col("sessions.id") == F.col("session_stats.session_id"), "left")
    .filter(F.col("session_stats.unique_pages") >= 6)
    .select(
        F.col("sessions.id"),
        F.col("sessions.browser"),
        F.col("session_stats.unique_pages"),
        F.col("session_stats.cart_count"),
    )
    .orderBy("browser", F.col("unique_pages").desc())
)
