from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("status") == "RETURNED") & (F.col("amount") <= 830))
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(
        F.countDistinct("amount").alias("unique_channels"),
        F.sum(F.when(F.col("status") == "PENDING", F.col("shipping_fee")).otherwise(0)).alias("pending_shipping_fee"),
    )
    .orderBy(F.col("pending_shipping_fee").desc())
)
