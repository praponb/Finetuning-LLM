from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("i.product_id").isin([2, 6, 9, 10]))
    .groupBy(F.col("p.brand"))
    .agg(
        F.countDistinct(F.col("i.quantity")).alias("unique_stores"),
        F.count(F.col("i.quantity")).alias("quantity_count"),
    )
    .orderBy("unique_stores", "brand")
    .limit(1)
)
