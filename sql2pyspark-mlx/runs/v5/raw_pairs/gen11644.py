from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.min("last_restock").alias("min_last_restock"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = (
    df_products.alias("products")
    .join(inventory_by_product.alias("t"), F.col("products.id") == F.col("t.product_id"), "left")
    .select(
        F.col("products.id"),
        F.col("products.cost"),
        F.col("t.min_last_restock"),
        F.col("t.max_quantity"),
    )
)
