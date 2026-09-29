from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date").between("2025-06-01", "2025-07-20"))
    .groupBy(
        "category",
        F.when(F.col("price") >= 1050, "high").when(F.col("price") >= 590, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(F.count("*").alias("product_count"))
    .filter(F.col("product_count") >= 3)
    .orderBy(F.col("category").desc(), "price_bucket")
    .limit(1)
)
