from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("s.browser") == "Chrome")
    .groupBy(F.col("s.customer_id"))
    .agg(
        F.sum(F.when(F.col("we.page") == "/checkout", 1).otherwise(0)).alias("checkout_count"),
        F.avg(F.col("we.duration_sec")).alias("avg_duration_sec"),
    )
)
