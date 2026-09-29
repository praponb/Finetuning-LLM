from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.product_id") == 5)
    .groupBy(F.quarter(F.col("ss.sale_date")).alias("sale_quarter"))
    .agg(F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"))
    .orderBy(F.col("sale_quarter").desc())
)
