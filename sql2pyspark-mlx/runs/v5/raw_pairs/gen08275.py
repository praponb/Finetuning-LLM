from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "right")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.brand"))
    .agg(F.min(F.col("ss.units")).alias("min_units"))
    .orderBy(F.col("min_units").desc(), F.col("brand").desc())
    .limit(3)
)
