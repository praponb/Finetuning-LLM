from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "inner")
    .groupBy(F.col("store_sales.product_id"))
    .agg(
        F.max(F.col("inventory.quantity")).alias("max_quantity"),
        F.min(F.col("inventory.last_restock")).alias("first_last_restock"),
    )
    .orderBy(F.col("max_quantity").desc(), F.col("product_id").desc())
)
