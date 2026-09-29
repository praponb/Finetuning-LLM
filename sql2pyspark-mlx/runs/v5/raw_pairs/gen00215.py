from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter((F.year(F.col("i.last_restock")) >= 2025) | (F.col("ss.product_id") == 9))
    .select(F.col("i.quantity"), F.col("ss.units"), F.col("ss.revenue"))
)
