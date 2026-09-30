from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.year("launch_date") == 2023)
    .select("id", "price", F.explode("tags").alias("label"))
)
