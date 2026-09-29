from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.brand"))
    .agg(
        F.count(F.col("i.quantity")).alias("quantity_count"),
        F.max(F.col("i.last_restock")).alias("last_last_restock"),
    )
    .orderBy("brand")
)
