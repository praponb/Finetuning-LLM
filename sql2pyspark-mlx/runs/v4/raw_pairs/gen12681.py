from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(F.max("price").alias("max_price"))
    .filter(F.col("max_price") > 1050)
)
