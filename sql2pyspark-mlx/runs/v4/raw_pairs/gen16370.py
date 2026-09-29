from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("c.segment"))
    .agg(
        (F.max(F.col("o.shipping_fee")) - F.min(F.col("o.shipping_fee"))).alias("shipping_fee_range"),
        F.round(F.avg(F.col("o.discount")), 2).alias("avg_discount"),
    )
    .orderBy("shipping_fee_range")
)
