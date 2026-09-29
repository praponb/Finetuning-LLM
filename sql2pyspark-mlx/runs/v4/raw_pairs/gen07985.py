from pyspark.sql import functions as F

valid_order_items = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .withColumn("line_total", (F.col("unit_price") + F.col("quantity")))
)

agg = (
    valid_order_items
    .groupBy("product_id")
    .agg(F.countDistinct("line_total").alias("unique_products"))
)

result = (
    agg
    .select("product_id", "unique_products")
    .orderBy(F.col("product_id").desc())
)
