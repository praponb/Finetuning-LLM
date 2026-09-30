from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .select(F.col("o.id"), F.col("o.order_date"), F.col("c.email"), F.col("s.region"))
    .orderBy(F.col("region").desc())
)
