from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .groupBy(F.col("c.city"))
    .agg(
        F.count(F.col("o.discount")).alias("discount_count"),
        F.round(F.stddev(F.col("o.shipping_fee")), 2).alias("stddev_shipping_fee"),
    )
    .orderBy(F.col("stddev_shipping_fee").desc(), "city")
)
