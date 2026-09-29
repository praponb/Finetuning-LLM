from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "right")
    .filter(F.col("s.city") != "Singapore")
    .groupBy(F.col("p.brand"))
    .agg(
        (F.max(F.col("ss.revenue")) - F.min(F.col("ss.revenue"))).alias("revenue_range"),
        F.round(F.avg(F.col("ss.units")), 2).alias("avg_units"),
    )
    .orderBy(F.col("revenue_range").desc(), "brand")
    .limit(20)
)
