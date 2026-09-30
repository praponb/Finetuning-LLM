from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.segment").isin(["SMB", "Corporate", "Consumer"]))
    .groupBy(F.col("c.is_active"))
    .agg(F.min(F.col("o.shipping_fee")).alias("min_shipping_fee"))
)
