from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(
        F.when(F.col("c.age") >= 56, "high").when(F.col("c.age") >= 31, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(F.min(F.col("we.duration_sec")).alias("min_duration_sec"))
    .filter(F.col("min_duration_sec") > 250)
    .orderBy("age_bucket")
)
