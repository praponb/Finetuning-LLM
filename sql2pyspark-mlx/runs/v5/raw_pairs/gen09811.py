from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date").between("2023-03-01", "2024-06-01"))
    .select("id", "price", F.explode("tags").alias("tag_name"))
)
