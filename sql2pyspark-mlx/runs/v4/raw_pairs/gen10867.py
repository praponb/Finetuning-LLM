from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "left")
    .groupBy(F.col("p.id"), F.col("p.brand"))
    .agg(
        F.count(F.col("i.id")).alias("inventory_count"),
        F.min(F.col("i.last_restock")).alias("min_last_restock"),
        F.max(F.col("i.quantity")).alias("max_quantity"),
    )
    .orderBy("max_quantity")
)
