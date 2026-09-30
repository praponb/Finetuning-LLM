from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .select(F.col("o.customer_id"), F.col("c.age"), F.col("c.country"))
)
