from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.city").isNotNull())
    .select(F.col("s.device"), F.col("c.name"))
    .orderBy("device")
)
