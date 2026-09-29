from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Initech", "Acme"]))
    .select("id", "stock_qty", F.round("price", 1).alias("price_rounded"))
)
