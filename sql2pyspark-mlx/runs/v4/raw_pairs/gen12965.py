from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "left")
    .select(F.col("s.id"), F.col("s.start_date"), F.col("c.email"), F.col("c.segment"))
    .orderBy("start_date")
)
