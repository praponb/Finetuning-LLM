from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.when(F.col("c.credit_limit") >= 17200, "high").when(F.col("c.credit_limit") >= 3700, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(F.avg(F.col("we.duration_sec")).alias("avg_duration_sec"))
    .filter(F.col("avg_duration_sec") > 340)
    .orderBy("credit_limit_bucket", F.col("avg_duration_sec").desc())
)
