from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "outer")
    .filter(F.col("inventory.product_id") == 6)
    .groupBy(F.col("stores.city"))
    .agg(F.max(F.col("inventory.quantity")).alias("max_quantity"))
    .filter(F.col("max_quantity") > 220)
)
