from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "inner")
    .filter(F.col("inventory.last_restock") >= "2023-06-01")
    .groupBy(
        F.col("store_sales.product_id"),
        F.when(F.col("store_sales.units") >= 42, "high").when(F.col("store_sales.units") >= 9, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(F.max(F.col("inventory.quantity")).alias("max_quantity"))
)
