from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter((F.col("s.city") != "Chiang Mai") & F.col("ss.product_id").isin([1, 4, 6, 10]))
    .select(F.col("s.region"), F.col("ss.revenue"))
)
