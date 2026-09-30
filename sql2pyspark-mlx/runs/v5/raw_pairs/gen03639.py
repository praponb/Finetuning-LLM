from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .filter(F.length(F.col("stores.store_name")) < 13)
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.product_id"),
        F.col("stores.store_name"),
        F.col("products.category"),
        F.round(F.col("store_sales.units"), 1).alias("units_rounded"),
    )
    .orderBy(F.col("id").desc(), "units_rounded")
    .limit(20)
)
