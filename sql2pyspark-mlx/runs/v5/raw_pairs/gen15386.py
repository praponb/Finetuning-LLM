from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .groupBy(F.col("c.segment"))
    .agg(F.round(F.stddev(F.col("we.duration_sec")), 2).alias("stddev_duration_sec"))
    .orderBy(F.col("stddev_duration_sec").desc(), "segment")
    .limit(50)
)
