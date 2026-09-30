from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "left")
    .filter(F.col("store_sales.product_id").isin([2, 7, 10]))
    .groupBy(F.col("store_sales.product_id"))
    .agg(
        F.count("*").alias("total_count"),
        F.countDistinct(F.col("inventory.quantity")).alias("unique_stores"),
    )
    .orderBy(F.col("product_id").desc(), F.col("total_count").desc())
    .limit(5)
)
