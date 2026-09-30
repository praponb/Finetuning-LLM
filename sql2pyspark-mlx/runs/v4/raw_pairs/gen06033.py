from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .filter(F.col("product_id") == 2)
    .groupBy("product_id")
    .agg(F.count("quantity").alias("quantity_count"))
)

result = (
    df_products.alias("products")
    .join(inventory_by_product.alias("inventory_by_product"), F.col("products.id") == F.col("inventory_by_product.product_id"), "inner")
    .filter(F.col("inventory_by_product.quantity_count") >= 4)
    .select(
        F.col("products.id"),
        F.col("products.category"),
        F.col("inventory_by_product.quantity_count"),
    )
)
