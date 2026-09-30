from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") == "Grocery")
    .select(F.explode("tags").alias("label"))
    .groupBy("label")
    .agg(F.count("*").alias("product_count"))
)
