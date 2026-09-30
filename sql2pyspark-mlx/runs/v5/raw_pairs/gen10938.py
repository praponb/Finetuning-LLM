from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.lower(F.col("s.store_name")).like("%irp%"))
    .groupBy(F.col("s.city"))
    .agg(F.min(F.col("ss.revenue")).alias("min_revenue"))
    .filter(F.col("min_revenue") >= 2500)
    .orderBy("min_revenue")
)
