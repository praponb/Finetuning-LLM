from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .groupBy(F.date_format(F.col("s.started_at"), "yyyy-MM").alias("started_month"))
    .agg(
        F.count(F.when(F.col("we.event_type") == "purchase", 1)).alias("purchase_count"),
    )
)
