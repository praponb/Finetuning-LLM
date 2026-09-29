from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "right")
    .groupBy(
        F.col("o.customer_id"),
        F.when(F.col("o.shipping_fee") >= 36, "high").when(F.col("o.shipping_fee") >= 10, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(F.countDistinct(F.col("p.amount")).alias("unique_statuss"))
    .filter(F.col("unique_statuss") > 5)
    .orderBy(F.col("shipping_fee_bucket").desc(), "customer_id")
    .limit(5)
)
