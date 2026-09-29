from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("channel") == "store") & ((F.col("order_date") >= "2025-01-01") | F.col("status").isin(["SHIPPED", "COMPLETED"])))
    .withColumn("shipping_fee_rounded", F.round("shipping_fee", 0))
)
