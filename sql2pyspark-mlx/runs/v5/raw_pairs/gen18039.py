from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .select(F.col("o.shipping_fee"), F.col("c.name"), F.col("s.city"), F.col("s.sqft"))
)
