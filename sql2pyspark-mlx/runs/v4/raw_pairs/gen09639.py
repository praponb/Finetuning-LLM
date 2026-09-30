from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") < 290) & (F.col("quantity") <= 5))
    .withColumn("unit_price_tier", F.when(F.col("unit_price") >= 300, "High").when(F.col("unit_price") >= 80, "Medium").otherwise("Low"))
    .withColumn("unit_price_rounded", F.round("unit_price", 0))
)
