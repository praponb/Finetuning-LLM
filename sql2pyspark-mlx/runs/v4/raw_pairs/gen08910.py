from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("ss.product_id"),
        F.col("s.region"),
        F.coalesce(F.col("s.store_name"), F.lit("N/A")).alias("store_name_filled"),
    )
    .orderBy(F.col("product_id").desc())
)
