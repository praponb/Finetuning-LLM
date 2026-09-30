from pyspark.sql import functions as F

result = (
    df_orders
    .filter(((F.col("order_date") <= "2024-09-12") & ~F.col("channel").isin(["store", "web", "partner"])) | (F.col("shipping_fee") < 26))
    .withColumn("shipping_fee_band", F.when(F.col("shipping_fee") >= 38, "High").when(F.col("shipping_fee") >= 14, "Medium").otherwise("Low"))
)
