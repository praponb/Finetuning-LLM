from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .groupBy(F.col("stores.city"), F.col("stores.region"))
    .agg(
        F.round(F.avg(F.col("store_sales.units")), 2).alias("avg_units"),
        F.count(F.col("store_sales.units")).alias("units_count"),
    )
)
