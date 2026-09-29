from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.customer_id").isNotNull())
    .select(F.col("s.status"), F.col("c.segment"))
    .orderBy("segment", "status")
)
