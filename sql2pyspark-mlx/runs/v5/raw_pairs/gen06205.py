from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .groupBy(F.col("c.is_active"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("we.event_type") == "add_to_cart", 1).otherwise(0))) / F.count("*")), 1).alias("pct_add_to_cart"),
        F.min(F.col("we.duration_sec")).alias("min_duration_sec"),
    )
    .orderBy(F.col("pct_add_to_cart").desc(), "min_duration_sec")
)
