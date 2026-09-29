from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.col("event_type") != "click").alias("we"), F.col("s.id") == F.col("we.session_id"), "left_semi")
    .filter(F.hour(F.col("s.started_at")).between(6, 13))
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("s.browser"),
        F.col("s.device"),
    )
    .orderBy(F.col("customer_id").desc())
)
