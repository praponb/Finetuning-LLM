from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_inventory.alias("inventory"), F.col("inventory.product_id") == F.col("products.id"), "left")
    .filter(F.size(F.col("products.tags")) > 2)
    .groupBy(F.col("products.id"), F.col("products.product_name"))
    .agg(
        F.count(F.col("inventory.id")).alias("inventory_count"),
        F.round(F.avg(F.col("inventory.quantity")), 2).alias("avg_quantity"),
    )
    .filter(F.col("inventory_count") >= 2)
    .orderBy(F.col("inventory_count").desc())
)
