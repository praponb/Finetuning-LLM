from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.length("product_name") >= 12)
    .select("id", "category", "product_name", F.explode("tags").alias("tag_name"))
)
