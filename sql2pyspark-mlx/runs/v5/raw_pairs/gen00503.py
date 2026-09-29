from pyspark.sql import functions as F

valid_order_items = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("unit_price_tier", F.when(F.col("unit_price") >= 370, "High").when(F.col("unit_price") >= 100, "Medium").otherwise("Low"))
)

summary = (
    valid_order_items
    .groupBy("order_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    summary
    .filter(F.col("quantity_range") < 73)
    .select("order_id", "quantity_range")
    .orderBy("order_id", F.col("quantity_range").desc())
    .limit(5)
)
