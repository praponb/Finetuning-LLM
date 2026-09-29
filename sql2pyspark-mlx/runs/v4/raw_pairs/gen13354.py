from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.col("customer_id").isin([17, 20, 32, 40])).alias("s"), F.col("we.session_id") == F.col("s.id"), "left_anti")
    .select(
        F.col("we.id"),
        F.col("we.session_id"),
        F.col("we.page"),
        F.col("we.event_time"),
    )
)
