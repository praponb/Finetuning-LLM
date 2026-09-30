from pyspark.sql import functions as F

filtered_orders = (
    df_orders
    .filter((F.month("order_date").isin([1, 6, 9]) & (F.col("amount") > 1140)) | (F.col("shipping_fee") > 38))
    .withColumn("is_web", F.when(F.col("channel") == "web", 1).otherwise(0))
)

agg = (
    filtered_orders
    .groupBy("status")
    .agg(F.min("shipping_fee").alias("min_shipping_fee"))
)

result = (
    agg
    .filter(F.col("min_shipping_fee") >= 14)
    .select("status", "min_shipping_fee")
)
