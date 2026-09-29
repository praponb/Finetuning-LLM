from pyspark.sql import functions as F

dim_totals = (
    df_store_sales
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["category", "brand"], ["category"]],
        F.col("p.category").alias("category"),
        F.col("p.brand").alias("brand"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("src.revenue")).alias("min_revenue"),
        F.min(F.col("src.units")).alias("min_units"),
        F.grouping_id().alias("level_id"),
    )
)

result = dim_totals.select("category", "brand", "num_rows", "min_units", "level_id")
