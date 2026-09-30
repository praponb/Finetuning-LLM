from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.is_active"))
    .agg(F.sum(F.when(F.col("s.country") == "DE", 1).otherwise(0)).alias("de_count"))
    .orderBy("de_count")
)
