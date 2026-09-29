from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("order_id").isin([2, 16, 20, 36]))
    .withColumn("line_total", F.round((F.col("unit_price") / F.col("quantity")), 2))
    .withColumn("least_unit_price_quantity", F.least("unit_price", "quantity"))
    .orderBy(F.col("quantity").desc(), F.col("order_id").desc())
)
