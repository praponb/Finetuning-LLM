from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("brand") == "Initech") & (F.col("brand") == "Initech"))
    .select("id", "stock_qty", F.floor("price").alias("price_whole"))
)
