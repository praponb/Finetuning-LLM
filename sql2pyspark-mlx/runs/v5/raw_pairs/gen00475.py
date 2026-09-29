from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .filter(F.col("order_id").isin([4, 40]))
    .groupBy("product_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.countDistinct("unit_price").alias("unique_products"),
    )
)

result = (
    df_products.alias("products")
    .join(product_summary.alias("t"), F.col("products.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.unique_products") > 8)
    .select(
        F.col("products.id"),
        F.col("products.category"),
        F.col("t.quantity_count"),
        F.col("t.unique_products"),
    )
    .orderBy("category")
)
