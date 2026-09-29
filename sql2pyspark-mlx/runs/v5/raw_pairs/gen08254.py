from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .filter(F.col("p.stock_qty") != 128)
    .groupBy(F.col("p.brand"))
    .agg(F.min(F.col("ss.units")).alias("min_units"))
    .orderBy(F.col("min_units").desc())
)
