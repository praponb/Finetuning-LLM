from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "left")
    .groupBy(F.col("store_sales.product_id"))
    .agg(F.round(F.avg(F.col("inventory.quantity")), 2).alias("avg_quantity"))
    .filter(F.col("avg_quantity") > 51)
)
