from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.device").isin(["mobile", "desktop"]))
    .groupBy(F.col("c.segment"), F.col("c.city"))
    .agg(F.round(F.avg(F.col("we.duration_sec")), 2).alias("avg_duration_sec"))
)
