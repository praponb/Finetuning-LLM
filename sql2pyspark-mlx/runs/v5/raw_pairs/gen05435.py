from pyspark.sql import functions as F

by_dimension = (
    df_inventory
    .alias("src")
    .join(df_products.alias("dim"), F.col("src.product_id") == F.col("dim.id"), "inner")
    .groupBy(F.col("dim.category").alias("category"))
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("src.quantity")).alias("max_quantity"),
    )
)

result = by_dimension.select("category", "rows_total", "max_quantity")
