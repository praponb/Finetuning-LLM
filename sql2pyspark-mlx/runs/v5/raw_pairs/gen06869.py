from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("o.customer_id").isin([3, 16, 19, 23]))
    .groupBy(F.col("c.is_active"))
    .agg(
        F.count("*").alias("total_count"),
        F.min(F.col("o.shipping_fee")).alias("min_shipping_fee"),
    )
    .filter(F.col("min_shipping_fee") > 37)
    .orderBy("min_shipping_fee", F.col("total_count").desc())
)
