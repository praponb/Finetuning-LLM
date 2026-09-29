from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") != "RETURNED")
    .groupBy(
        F.when(F.col("amount") >= 1600, "high").when(F.col("amount") >= 480, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "RETURNED", F.col("shipping_fee")).otherwise(0)).alias("returned_shipping_fee"),
        F.min("order_date").alias("first_order_date"),
    )
    .orderBy("returned_shipping_fee")
)
