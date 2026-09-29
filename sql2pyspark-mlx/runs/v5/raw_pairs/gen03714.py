from pyspark.sql import functions as F

by_dimension = (
    df_store_sales
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.category").alias("category"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("s.revenue")), 2).alias("total_revenue"),
        F.min(F.col("s.units")).alias("min_units"),
    )
)

result = by_dimension.select("category", "num_rows", "min_units")
