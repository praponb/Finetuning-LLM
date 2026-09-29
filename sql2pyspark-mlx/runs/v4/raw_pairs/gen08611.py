from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") <= "2023-05-01")
    .select("id", "product_name", "category", F.explode("tags").alias("tag_name"))
)
