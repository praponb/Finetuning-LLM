from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("s.browser").isin(["Chrome", "Edge"]))
    .select(F.col("we.event_type"), F.col("s.customer_id"))
    .orderBy(F.col("customer_id").desc(), F.col("event_type").desc())
)
