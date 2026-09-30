from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.stock_qty") != 317)
    .groupBy(F.col("s.city"))
    .agg(F.countDistinct(F.col("ss.revenue")).alias("unique_products"))
    .filter(F.col("unique_products") > 4)
)
