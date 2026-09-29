from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.browser").isin(["Firefox", "Safari"]) & F.col("we.session_id").isin([8, 34]))
    .select(F.col("s.device"), F.col("s.customer_id"), F.col("we.event_time"))
    .orderBy("device")
)
