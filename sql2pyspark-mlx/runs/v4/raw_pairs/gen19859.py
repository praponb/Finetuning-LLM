from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "left")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("ss.product_id").isin([5, 9]))
    .groupBy(F.col("s.region"))
    .agg(
        F.round(F.avg(F.col("ss.revenue")), 2).alias("avg_revenue"),
        F.max(F.col("ss.units")).alias("max_units"),
    )
    .orderBy("avg_revenue")
)
