from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.avg("shipping_fee").alias("avg_shipping_fee"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count"),
        F.max("amount").alias("max_amount"),
    )
    .orderBy(F.col("max_amount").desc())
)
