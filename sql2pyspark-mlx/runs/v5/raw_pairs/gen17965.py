from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("i.last_restock").between("2023-11-01", "2025-07-01") & (F.col("i.last_restock") <= "2023-07-01"))
    .select(F.col("i.id"), F.col("i.product_id"), F.col("ss.store_id"))
    .orderBy("store_id")
)
