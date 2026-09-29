from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "right")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(~F.col("c.city").isin(["Tokyo", "London", "Bangkok"]))
    .groupBy(F.col("s.country"))
    .agg(F.round(F.stddev(F.col("we.duration_sec")), 2).alias("stddev_duration_sec"))
    .filter(F.col("stddev_duration_sec") >= 55)
)
