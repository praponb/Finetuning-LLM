from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.product_id").isin([2, 4, 9, 10]))
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("s.sqft"),
        F.col("s.store_name"),
        F.col("s.region"),
        F.floor(F.col("s.sqft")).alias("sqft_whole"),
    )
    .orderBy(F.col("store_name").desc())
)
