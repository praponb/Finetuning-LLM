from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.count("*").alias("product_count"),
        F.min("launch_date").alias("first_launch_date"),
    )
    .filter(F.col("product_count") >= 2)
    .orderBy(F.col("product_count").desc(), "first_launch_date")
)
