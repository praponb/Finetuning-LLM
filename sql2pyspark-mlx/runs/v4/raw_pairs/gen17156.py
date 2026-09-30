from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("we.session_id").isin([7, 22]))
    .groupBy(F.col("s.country"))
    .agg(F.sort_array(F.collect_set(F.col("we.event_type"))).alias("event_type_list"))
    .orderBy(F.col("country").desc())
)
