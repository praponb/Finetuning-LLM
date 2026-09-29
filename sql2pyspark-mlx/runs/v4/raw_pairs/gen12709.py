from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.upper(F.trim("product_name")) == "RUNNING SHOES")
    .select("id", "product_name", F.explode("tags").alias("label"))
)
