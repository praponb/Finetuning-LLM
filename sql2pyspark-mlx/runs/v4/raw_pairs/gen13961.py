from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") <= 206)
    .select("stock_qty", F.explode("tags").alias("label"))
    .groupBy("label")
    .agg(F.sum("stock_qty").alias("total_stock"))
)
