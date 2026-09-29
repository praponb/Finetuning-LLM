from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand") != "Acme")
    .select("id", "product_name", "brand", F.explode("tags").alias("label"))
)
