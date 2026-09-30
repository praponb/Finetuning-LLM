from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("i.store_id").isin([4, 5]) & F.col("ss.product_id").isin([2, 3, 4, 10]))
    .select(F.col("i.id"), F.col("i.quantity"), F.col("ss.revenue"))
    .orderBy("revenue")
)
