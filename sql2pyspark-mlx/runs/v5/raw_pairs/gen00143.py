from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.country"))
    .agg(F.count(F.col("o.shipping_fee")).alias("shipping_fee_count"))
    .filter(F.col("shipping_fee_count") > 4)
)
