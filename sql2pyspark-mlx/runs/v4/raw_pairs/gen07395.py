from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.upper("product_name").like("%SB-%"))
    .select("id", "brand", "price", F.explode("tags").alias("label"))
)
