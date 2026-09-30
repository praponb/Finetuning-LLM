from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(247, 320))
    .select("id", "category", F.explode("tags").alias("label"))
)
