from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "left")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("i.product_id").isin([2, 3, 9, 10]))
    .groupBy(F.col("p.category"), F.col("p.brand"))
    .agg(
        F.max(F.col("i.last_restock")).alias("last_last_restock"),
        F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"),
    )
)
