from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("i.last_restock") >= "2024-07-01")
    .groupBy(F.col("p.brand"))
    .agg(
        F.sum(F.col("i.quantity")).alias("total_quantity"),
        F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"),
    )
    .orderBy(F.col("total_quantity").desc(), "brand")
    .limit(10)
)
