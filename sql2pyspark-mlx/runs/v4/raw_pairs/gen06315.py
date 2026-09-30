from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.product_id") == 4)
    .groupBy(F.quarter(F.col("ss.sale_date")).alias("sale_quarter"))
    .agg(F.count(F.col("i.quantity")).alias("quantity_count"))
    .orderBy(F.col("quantity_count").desc(), F.col("sale_quarter").desc())
)
