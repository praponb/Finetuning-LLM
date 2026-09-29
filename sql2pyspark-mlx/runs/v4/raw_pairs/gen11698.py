from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.cost").between(310, 640))
    .groupBy(F.col("p.category"))
    .agg(
        F.round(F.stddev(F.col("ss.revenue")), 2).alias("stddev_revenue"),
        (F.max(F.col("ss.revenue")) - F.min(F.col("ss.revenue"))).alias("revenue_range"),
    )
    .filter(F.col("stddev_revenue") >= 47)
    .orderBy("revenue_range")
)
