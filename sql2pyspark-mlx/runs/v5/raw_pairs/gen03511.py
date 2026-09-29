from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "outer")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.segment") == "Corporate")
    .groupBy(F.col("c.city"), F.col("c.country"))
    .agg(F.sort_array(F.collect_set(F.col("we.page"))).alias("page_list"))
)
