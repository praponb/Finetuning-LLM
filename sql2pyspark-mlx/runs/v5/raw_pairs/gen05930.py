from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("inventory.store_id"), "left")
    .filter(F.col("inventory.product_id").isin([1, 2, 4, 8]))
    .groupBy(F.month(F.col("stores.opened_date")).alias("opened_month"))
    .agg(F.avg(F.col("inventory.quantity")).alias("avg_quantity"))
    .filter(F.col("avg_quantity") >= 230)
)
