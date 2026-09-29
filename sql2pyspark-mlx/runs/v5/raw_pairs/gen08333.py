from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.when(F.col("shipping_fee") >= 37, "high").when(F.col("shipping_fee") >= 9, "mid").otherwise("low").alias("shipping_fee_bucket"))
    .pivot("status", ["SHIPPED", "RETURNED"])
    .agg(F.avg("shipping_fee"))
)
