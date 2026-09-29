from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").isNotNull())
    .select("id", "product_name", F.explode("tags").alias("tag_name"))
)
