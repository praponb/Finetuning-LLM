from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.count("*").alias("order_item_count"),
    )
)

result = (
    df_products.alias("products")
    .join(product_stats.alias("t"), F.col("products.id") == F.col("t.product_id"), "left")
    .filter(F.col("t.max_quantity") > 3.4)
    .select(
        F.col("products.id"),
        F.col("products.brand"),
        F.col("products.product_name"),
        F.col("t.max_quantity"),
        F.col("t.order_item_count"),
    )
    .orderBy("product_name")
)
