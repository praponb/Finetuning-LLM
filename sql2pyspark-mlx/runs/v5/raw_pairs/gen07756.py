from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter((F.col("launch_date") > "2023-10-01") & F.col("product_name").like("%Bottle%"))
)

agg = (
    recent_products
    .groupBy("supplier_id", "category")
    .agg(F.count("cost").alias("cost_count"))
)

result = agg.orderBy("category", F.col("supplier_id").desc())
