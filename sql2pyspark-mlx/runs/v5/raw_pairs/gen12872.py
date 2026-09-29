from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(260, 300))
    .withColumn("unit_price_level", F.when(F.col("unit_price") >= 370, "High").when(F.col("unit_price") >= 170, "Medium").otherwise("Low"))
    .withColumn("unit_price_rounded", F.round("unit_price", 1))
)
