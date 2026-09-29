from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "right")
    .select(F.col("ss.id"), F.col("ss.product_id"), F.col("p.brand"), F.col("s.city"))
    .orderBy(F.col("product_id").desc())
)
