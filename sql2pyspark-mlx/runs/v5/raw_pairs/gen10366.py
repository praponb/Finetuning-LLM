from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("channel") != "mobile") | (F.col("amount") > 720))
    .groupBy("channel")
    .agg(
        F.sum("shipping_fee").alias("total_shipping_fee"),
        F.countDistinct("discount").alias("unique_channels"),
    )
)
