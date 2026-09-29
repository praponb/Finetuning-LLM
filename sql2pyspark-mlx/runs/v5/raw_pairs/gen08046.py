from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("unit_price").between(170, 320) & (F.col("quantity") < 8))
    .withColumn("unit_price_adjusted", F.round((F.col("unit_price") * 1.1), 2))
)
