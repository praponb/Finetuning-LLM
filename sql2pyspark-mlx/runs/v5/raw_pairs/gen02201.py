from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "inner")
    .groupBy(F.month(F.col("stores.opened_date")).alias("opened_month"))
    .agg(F.countDistinct(F.col("inventory.quantity")).alias("unique_stores"))
)
