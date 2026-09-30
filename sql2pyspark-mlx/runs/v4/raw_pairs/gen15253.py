from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("customer_id")
    .agg(
        F.corr("amount", "shipping_fee").alias("corr_amount_shipping_fee"),
        F.sum(F.when(F.col("channel") == "store", F.col("amount"))).alias("store_total_amount"),
        F.round(F.avg("discount"), 2).alias("avg_discount"),
    )
    .filter(F.col("store_total_amount") >= 0)
)
