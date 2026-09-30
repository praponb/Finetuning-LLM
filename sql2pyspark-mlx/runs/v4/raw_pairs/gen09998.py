from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.array_contains(F.col("p.tags"), "new"))
    .groupBy(F.col("s.city"))
    .agg(F.round(F.stddev(F.col("ss.revenue")), 2).alias("stddev_revenue"))
    .filter(F.col("stddev_revenue") >= 63)
    .orderBy(F.col("stddev_revenue").desc())
)
