from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "right")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("products.price") >= 950)
    .groupBy(F.col("stores.city"))
    .agg(F.round(F.avg(F.col("store_sales.units")), 2).alias("avg_units"))
    .orderBy("city")
)
