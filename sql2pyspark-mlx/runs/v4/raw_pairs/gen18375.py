from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .groupBy(F.col("p.brand"))
    .agg(
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
        F.min(F.col("i.last_restock")).alias("min_last_restock"),
    )
)
