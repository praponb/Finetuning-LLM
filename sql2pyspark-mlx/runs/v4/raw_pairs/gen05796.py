from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .groupBy(F.col("c.is_active"))
    .agg(F.countDistinct(F.col("s.device")).alias("unique_devices"))
)
