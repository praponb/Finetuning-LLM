from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("amount") >= 1550, "LARGE").otherwise("SMALL").alias("amount_tier"),
        F.when(F.col("shipping_fee") >= 44, "HIGH").otherwise("LOW").alias("shipping_fee_tier"),
    )
    .agg(
        F.avg(F.when(F.col("status") == "RETURNED", F.col("amount"))).alias("returned_avg_amount"),
        F.corr("discount", "shipping_fee").alias("corr_discount_shipping_fee"),
        F.sum(F.when(F.col("channel") == "mobile", F.col("amount"))).alias("mobile_total_amount"),
    )
    .orderBy("returned_avg_amount")
)
