from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .groupBy("product_id")
    .agg(F.countDistinct("quantity").alias("unique_products"))
)

result = (
    df_products.alias("products")
    .join(product_summary.alias("t"), F.col("products.id") == F.col("t.product_id"), "left")
    .select(F.col("products.cost"), F.col("products.price"), F.col("t.unique_products"))
    .orderBy(F.col("unique_products").desc(), F.col("price").desc())
)
