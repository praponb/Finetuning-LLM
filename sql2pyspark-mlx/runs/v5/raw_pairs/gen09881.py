from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") >= "2023-11-01")
    .select("id", "price", F.explode("tags").alias("label"))
)
