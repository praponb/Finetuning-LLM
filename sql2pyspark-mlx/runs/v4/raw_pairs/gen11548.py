from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.year("order_date") == 2025) & (F.col("customer_id").isin([7, 29]) | F.col("amount").isNotNull()))
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.sum("shipping_fee").alias("total_shipping_fee"),
        F.countDistinct("discount").alias("unique_channels"),
    )
)
