from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.store_id").isin([2, 3, 4, 5]))
    .groupBy(F.date_trunc("MONTH", F.col("ss.sale_date")).alias("sale_month"))
    .agg(F.max(F.col("i.last_restock")).alias("latest_last_restock"))
    .orderBy(F.col("latest_last_restock").desc())
)
