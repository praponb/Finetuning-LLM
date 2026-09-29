from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "inner")
    .filter(F.col("p.supplier_id").isin([10, 17, 40]))
    .groupBy(F.col("p.id"), F.col("p.category"))
    .agg(
        F.count(F.col("i.id")).alias("inventory_count"),
        F.max(F.col("i.last_restock")).alias("last_last_restock"),
        F.count(F.col("i.quantity")).alias("quantity_count"),
    )
)
