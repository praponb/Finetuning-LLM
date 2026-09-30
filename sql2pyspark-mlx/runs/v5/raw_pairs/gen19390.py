from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.month(F.col("c.signup_date")).isin([3, 4, 6]))
    .groupBy(F.col("c.segment"))
    .agg(F.countDistinct(F.col("s.device")).alias("unique_devices"))
    .orderBy("unique_devices", "segment")
    .limit(5)
)
