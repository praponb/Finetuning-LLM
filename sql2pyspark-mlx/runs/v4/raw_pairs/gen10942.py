from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("c.is_active"))
    .select(F.col("o.id"), F.col("o.amount"), F.col("o.shipping_fee"), F.col("c.age"))
    .orderBy(F.col("age").desc())
)
