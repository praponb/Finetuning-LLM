from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("o.customer_id") == 36)
    .groupBy(
        F.col("c.city"),
        F.when(F.col("c.age") >= 60, "high").when(F.col("c.age") >= 25, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("o.status") == "CANCELLED", F.col("o.discount")).otherwise(0)).alias("cancelled_discount"),
        F.min(F.col("o.shipping_fee")).alias("min_shipping_fee"),
    )
)
