from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("product_id") == 10) | (F.col("order_id") == 20))
    .groupBy("product_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.max("unit_price").alias("max_unit_price"),
    )
    .orderBy(F.col("max_unit_price").desc(), F.col("product_id").desc())
    .limit(10)
)
