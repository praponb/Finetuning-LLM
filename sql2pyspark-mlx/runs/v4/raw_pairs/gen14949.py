from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "outer")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("s.region"), F.col("s.city"))
    .agg(
        (F.max(F.col("o.shipping_fee")) - F.min(F.col("o.shipping_fee"))).alias("shipping_fee_range"),
    )
)
