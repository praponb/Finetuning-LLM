from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "left")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .groupBy(F.col("stores.city"))
    .agg(F.round(F.stddev(F.col("inventory.quantity")), 2).alias("stddev_quantity"))
    .orderBy(F.col("city").desc(), "stddev_quantity")
)
