from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") >= 660)
    .select("id", "brand", "product_name", F.explode("tags").alias("tag_name"))
)
