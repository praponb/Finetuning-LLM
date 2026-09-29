from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") >= "2024-10-01")
    .select("id", "product_name", F.explode("tags").alias("label"))
)
