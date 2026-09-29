from pyspark.sql import functions as F

base = df_order_items.filter(F.col("unit_price").between(310, 400))

order_items_stats = (
    base
    .groupBy("product_id")
    .agg(F.min("unit_price").alias("min_unit_price"))
)

result = (
    order_items_stats
    .filter(F.col("min_unit_price") > 170)
    .orderBy(F.col("product_id").desc())
    .limit(5)
)
