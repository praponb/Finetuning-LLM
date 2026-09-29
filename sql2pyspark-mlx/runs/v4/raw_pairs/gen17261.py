from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "inner")
    .filter(F.col("inventory.quantity") <= 76)
    .groupBy(F.month(F.col("store_sales.sale_date")).alias("sale_month"))
    .agg(F.count("*").alias("inventory_count"))
    .orderBy(F.col("inventory_count").desc(), "sale_month")
    .limit(3)
)
