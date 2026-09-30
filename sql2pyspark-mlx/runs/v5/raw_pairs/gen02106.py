from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .filter(F.col("s.device") == "desktop")
    .groupBy(
        F.when(F.col("c.credit_limit") >= 16000, "high").when(F.col("c.credit_limit") >= 4500, "mid").otherwise("low").alias("credit_limit_bucket"),
        F.month(F.col("c.signup_date")).alias("signup_month"),
    )
    .agg(F.countDistinct(F.col("we.duration_sec")).alias("unique_event_types"))
    .orderBy("credit_limit_bucket")
)
