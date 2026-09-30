from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "outer")
    .filter(F.col("o.shipping_fee").between(26, 33))
    .groupBy(
        F.col("o.status"),
        F.when(F.col("o.shipping_fee") >= 43, "high").when(F.col("o.shipping_fee") >= 13, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(F.max(F.col("s.cost")).alias("max_cost"))
)
