from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.age") > 35)
    .groupBy(
        F.when(F.col("c.credit_limit") >= 17300, "high").when(F.col("c.credit_limit") >= 7800, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(F.count("*").alias("web_event_count"))
    .orderBy("credit_limit_bucket")
)
