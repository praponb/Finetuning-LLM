from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.lower(F.col("s.store_name")).like("%ld %"))
    .groupBy(F.col("s.region"))
    .agg(
        F.count(F.col("ss.revenue")).alias("revenue_count"),
        F.countDistinct(F.col("ss.units")).alias("unique_stores"),
    )
    .filter(F.col("unique_stores") >= 5)
    .orderBy(F.col("revenue_count").desc())
)
