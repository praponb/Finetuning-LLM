from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("launch_date") > "2024-10-01") & F.col("product_name").like("%Watc%"))
    .groupBy("category")
    .agg(F.count("price").alias("price_count"))
)
