from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .groupBy(F.col("stores.region"))
    .agg(F.avg(F.col("store_sales.units")).alias("avg_units"))
    .orderBy(F.col("avg_units").desc(), "region")
    .limit(50)
)
