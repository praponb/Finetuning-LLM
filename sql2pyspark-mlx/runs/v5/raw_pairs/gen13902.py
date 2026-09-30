from pyspark.sql import functions as F

result = (
    df_store_sales
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .rollup(F.col("p.brand").alias("brand"))
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("s.revenue")), 2).alias("avg_revenue"),
        F.max(F.col("s.units")).alias("max_units"),
        F.grouping_id().alias("gid"),
    )
)
