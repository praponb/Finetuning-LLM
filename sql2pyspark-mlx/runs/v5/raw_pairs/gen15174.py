from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .groupBy(
        F.when(F.col("c.credit_limit") >= 14800, "high").when(F.col("c.credit_limit") >= 3000, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.round(F.stddev(F.col("o.amount")), 2).alias("stddev_amount"),
        F.countDistinct(F.col("o.shipping_fee")).alias("unique_customers"),
    )
    .orderBy(F.col("unique_customers").desc(), "credit_limit_bucket")
    .limit(20)
)
