from pyspark.sql import functions as F

result = (
    df_store_sales
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.brand").alias("brand"))
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("src.revenue")).alias("min_revenue"),
        F.min(F.col("src.units")).alias("min_units"),
    )
    .orderBy(F.col("rows_total").asc_nulls_first())
)
