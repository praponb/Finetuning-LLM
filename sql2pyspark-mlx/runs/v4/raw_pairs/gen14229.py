from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") >= 640)
    .groupBy("category")
    .agg(
        F.count("*").alias("num_products"),
        F.min("launch_date").alias("min_launch_date"),
    )
    .filter(F.col("num_products") >= 9)
    .select("category", "num_products", "min_launch_date")
    .orderBy("min_launch_date", "category")
)
