from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("ss.store_id").isin([4, 5, 6]))
    .select(F.col("ss.product_id"), F.col("p.launch_date"), F.col("s.city"))
)
