from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "outer")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.customer_id").isNull())
    .groupBy(
        F.col("c.segment"),
        F.when(F.col("c.credit_limit") >= 16200, "high").when(F.col("c.credit_limit") >= 3500, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.max(F.col("o.discount")).alias("max_discount"),
        F.sum(F.col("o.shipping_fee")).alias("total_shipping_fee"),
    )
)
