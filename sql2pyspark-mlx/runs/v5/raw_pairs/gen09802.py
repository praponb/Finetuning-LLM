from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date").between("2024-11-17", "2025-09-01"))
    .select("id", "stock_qty", F.length("category").alias("category_length"))
)
