from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") >= 221)
    .select("id", "product_name", "brand", F.explode("tags").alias("label"))
)
