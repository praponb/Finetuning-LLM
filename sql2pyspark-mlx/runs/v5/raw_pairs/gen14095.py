from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") < 39)
    .groupBy(F.month("order_date").alias("order_month"), "channel")
    .agg(
        F.sum(F.when(F.col("channel") == "partner", 1).otherwise(0)).alias("partner_count"),
        F.avg("amount").alias("avg_amount"),
        F.avg("shipping_fee").alias("avg_shipping_fee"),
    )
    .orderBy(F.col("partner_count").desc())
)
