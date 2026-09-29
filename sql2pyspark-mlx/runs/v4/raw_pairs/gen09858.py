from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .filter(F.col("p.price") < 400)
    .groupBy(F.col("p.brand"))
    .agg(
        F.countDistinct(F.col("ss.revenue")).alias("unique_products"),
        F.count("*").alias("num_store_sales"),
    )
    .orderBy(F.col("num_store_sales").desc())
)
