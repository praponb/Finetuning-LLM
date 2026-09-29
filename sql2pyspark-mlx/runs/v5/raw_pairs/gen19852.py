from pyspark.sql import functions as F

result = (
    df_store_sales
    .alias("e")
    .join(df_products.alias("dim"), F.col("e.product_id") == F.col("dim.id"), "inner")
    .groupBy(F.col("dim.category").alias("category"))
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("e.units")).alias("min_units"),
    )
)
