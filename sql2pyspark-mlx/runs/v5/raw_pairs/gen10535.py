from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .filter(F.col("product_id").isin([2, 9, 10]))
    .groupBy("product_id")
    .agg(F.count("quantity").alias("quantity_count"))
)

result = (
    df_products.alias("p")
    .join(inventory_by_product.alias("ibp"), F.col("p.id") == F.col("ibp.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.brand"),
        F.col("p.stock_qty"),
        F.col("ibp.quantity_count"),
    )
)
