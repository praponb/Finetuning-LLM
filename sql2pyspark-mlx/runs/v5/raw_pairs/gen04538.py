from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("inventory.store_id").isin([1, 3, 5]))
    .groupBy(F.col("products.category"), F.col("products.brand"))
    .agg(
        F.avg(F.col("inventory.quantity")).alias("avg_quantity"),
        F.max(F.col("inventory.quantity")).alias("max_quantity"),
    )
    .orderBy(F.col("category").desc(), F.col("brand").desc())
    .limit(20)
)
