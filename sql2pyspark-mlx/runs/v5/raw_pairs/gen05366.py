from pyspark.sql import functions as F

result = (
    df_store_sales
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["category", "brand"], ["category"]],
        F.col("d.category").alias("category"),
        F.col("d.brand").alias("brand"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("src.revenue")), 2).alias("total_revenue"),
        F.grouping_id().alias("grouping_level"),
    )
)
