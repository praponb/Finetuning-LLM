from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .filter(F.col("ss.revenue").between(1100, 2600))
    .groupBy(F.col("p.brand"))
    .agg(
        F.round(F.stddev(F.col("ss.revenue")), 2).alias("stddev_revenue"),
        F.sum(F.col("ss.revenue")).alias("total_revenue"),
    )
    .filter(F.col("stddev_revenue") > 23)
    .orderBy(F.col("stddev_revenue").desc())
)
