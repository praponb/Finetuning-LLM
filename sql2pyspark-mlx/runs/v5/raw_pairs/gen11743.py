from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .groupBy(F.col("stores.region"))
    .agg(
        (F.max(F.col("store_sales.units")) - F.min(F.col("store_sales.units"))).alias("units_range"),
    )
    .filter(F.col("units_range") >= 41)
    .orderBy(F.col("region").desc())
    .limit(5)
)
