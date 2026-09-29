from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "outer")
    .filter(F.col("o.channel") == "partner")
    .groupBy(F.col("c.country"))
    .agg(
        F.count(F.col("o.discount")).alias("discount_count"),
        F.avg(F.col("o.shipping_fee")).alias("avg_shipping_fee"),
    )
    .orderBy(F.col("avg_shipping_fee").desc())
)
