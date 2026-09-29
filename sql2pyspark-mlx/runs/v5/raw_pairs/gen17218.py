from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("i.quantity").between(167, 221))
    .groupBy(F.col("s.region"))
    .agg(
        F.min(F.col("i.last_restock")).alias("first_last_restock"),
        F.max(F.col("i.quantity")).alias("max_quantity"),
    )
    .orderBy("region")
)
