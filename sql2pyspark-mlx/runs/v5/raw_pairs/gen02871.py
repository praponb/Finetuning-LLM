from pyspark.sql import functions as F

base = (
    df_order_items
    .filter((F.col("quantity").between(5, 6) & (F.col("quantity") > 5)) | (F.col("unit_price") > 230))
    .withColumn("least_unit_price_quantity", F.least("unit_price", "quantity"))
)

agg = (
    base
    .groupBy("product_id", "order_id")
    .agg(F.min("quantity").alias("min_quantity"))
)

result = (
    agg
    .filter(F.col("min_quantity") >= 3.4)
    .select("product_id", "min_quantity")
    .orderBy(F.col("min_quantity").desc())
)
