from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .groupBy(F.col("p.brand"))
    .agg(
        F.max(F.col("i.last_restock")).alias("latest_last_restock"),
        F.min(F.col("i.quantity")).alias("min_quantity"),
    )
    .orderBy(F.col("min_quantity").desc(), "brand")
    .limit(20)
)
