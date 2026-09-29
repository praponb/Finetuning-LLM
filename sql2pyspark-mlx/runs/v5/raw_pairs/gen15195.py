from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.year("order_date") == 2025)
    .groupBy(
        F.when(F.col("amount") >= 1530, "high").when(F.col("amount") >= 470, "mid").otherwise("low").alias("amount_bucket"),
        "status",
    )
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("shipping_fee")).otherwise(0)).alias("cancelled_shipping_fee"),
        F.max("order_date").alias("latest_order_date"),
        (F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"),
    )
    .orderBy("amount_bucket")
)
