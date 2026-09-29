from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("we.event_time") < "2023-12-01")
    .select(
        F.col("we.page"),
        F.col("we.event_time"),
        F.col("s.country"),
        F.col("s.browser"),
        F.col("c.country").alias("customer_country"),
    )
    .orderBy(F.col("country").desc())
)
