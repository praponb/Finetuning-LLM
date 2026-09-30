from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([26, 31]))
    .select("id", "product_name", "category", F.explode("tags").alias("tag_name"))
)
