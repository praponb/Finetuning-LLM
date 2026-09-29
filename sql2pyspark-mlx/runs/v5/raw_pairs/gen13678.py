from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.filter(F.col("signup_date") >= "2025-03-01").alias("c"), F.col("o.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("o.store_id").isNotNull())
    .select(F.col("o.id"), F.col("o.channel"), F.col("o.amount"))
)
