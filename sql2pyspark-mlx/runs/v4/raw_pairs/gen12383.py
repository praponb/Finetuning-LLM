from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.product_id").isin([1, 3, 5, 10]))
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("s.store_name"),
        F.col("s.sqft"),
    )
    .orderBy(F.col("product_id").desc(), F.col("id").desc())
)
