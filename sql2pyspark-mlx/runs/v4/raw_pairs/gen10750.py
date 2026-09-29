from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("ss.sale_date").between("2023-06-02", "2025-08-26"))
    .groupBy(F.col("p.brand"))
    .agg(
        F.round(F.stddev(F.col("ss.revenue")), 2).alias("stddev_revenue"),
        F.min(F.col("ss.units")).alias("min_units"),
    )
    .orderBy(F.col("stddev_revenue").desc(), "brand")
    .limit(50)
)
