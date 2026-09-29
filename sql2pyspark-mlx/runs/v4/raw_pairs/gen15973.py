from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.store_id") == 4)
    .select(
        F.col("ss.product_id"),
        F.col("s.store_name"),
        F.round(F.col("s.sqft"), 1).alias("sqft_rounded"),
    )
    .orderBy("sqft_rounded")
)
