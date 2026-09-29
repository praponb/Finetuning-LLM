from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .groupBy(F.col("s.region"))
    .agg(
        F.min(F.col("i.last_restock")).alias("min_last_restock"),
        F.countDistinct(F.col("i.quantity")).alias("unique_products"),
    )
    .orderBy("region")
    .limit(10)
)
