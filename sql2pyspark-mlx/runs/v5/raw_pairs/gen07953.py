from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.supplier_id"))
    .agg(
        F.min(F.col("i.last_restock")).alias("min_last_restock"),
        F.count("*").alias("inventory_count"),
    )
    .orderBy(F.col("min_last_restock").desc(), "inventory_count")
)
