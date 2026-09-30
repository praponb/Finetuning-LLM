from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("products.id") == F.col("inventory.product_id"), "inner")
    .groupBy(F.col("products.category"))
    .agg(
        F.max(F.col("inventory.last_restock")).alias("last_last_restock"),
        F.avg(F.col("inventory.quantity")).alias("avg_quantity"),
    )
)
