from pyspark.sql import functions as F

result = (
    df_store_sales
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.brand").alias("brand"))
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("src.units")).alias("total_units"),
        F.avg(F.col("src.revenue")).alias("avg_revenue"),
    )
)
