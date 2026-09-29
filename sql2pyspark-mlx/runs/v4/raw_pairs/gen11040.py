from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .groupBy(F.col("c.segment"))
    .agg(F.min(F.col("o.shipping_fee")).alias("min_shipping_fee"))
    .filter(F.col("min_shipping_fee") >= 23)
    .orderBy("min_shipping_fee", "segment")
    .limit(3)
)
