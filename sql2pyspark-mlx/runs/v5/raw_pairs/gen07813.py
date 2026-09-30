from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand") == "Initech")
    .select("id", "product_name", "category", F.explode("tags").alias("label"))
)
