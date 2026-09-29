from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.started_at") < "2025-06-14")
    .groupBy(F.col("s.device"))
    .agg(F.sort_array(F.collect_set(F.col("we.page"))).alias("page_list"))
    .orderBy("device")
)
