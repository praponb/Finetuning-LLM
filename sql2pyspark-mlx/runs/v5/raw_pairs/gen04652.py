from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.month(F.col("ss.sale_date")).isin([5, 6, 11]))
    .groupBy(F.col("ss.store_id"))
    .agg(F.min(F.col("i.quantity")).alias("min_quantity"))
)
