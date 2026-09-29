from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("price") < 680) & ((F.size("tags") > 1) | F.col("supplier_id").isNull()))
    .select("id", "product_name", "category", F.size("tags").alias("tags_count"))
    .orderBy("tags_count")
)
