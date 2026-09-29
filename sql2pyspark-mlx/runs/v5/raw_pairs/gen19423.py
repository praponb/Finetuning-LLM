from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.product_id").isin([2, 4, 8, 9]))
    .groupBy(F.col("ss.store_id"), F.month(F.col("ss.sale_date")).alias("sale_month"))
    .agg(F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"))
    .orderBy(F.col("sale_month").desc(), "avg_quantity")
)
