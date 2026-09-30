from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") >= 198)
    .select("id", "price", "category", F.explode("tags").alias("label"))
)
