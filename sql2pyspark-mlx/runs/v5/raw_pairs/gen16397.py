from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.browser") == "Firefox")
    .groupBy(F.col("s.device"))
    .agg(
        F.sort_array(F.collect_set(F.col("we.page"))).alias("page_list"),
        F.max(F.col("we.event_time")).alias("last_event_time"),
    )
    .orderBy("last_event_time")
)
