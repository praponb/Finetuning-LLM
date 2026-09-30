from pyspark.sql import functions as F

result = (
    df_store_sales
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["category", "brand"], ["category"], []],
        F.col("d.category").alias("category"),
        F.col("d.brand").alias("brand"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("e.revenue")).alias("min_revenue"),
        F.max(F.col("e.units")).alias("max_units"),
    )
)
