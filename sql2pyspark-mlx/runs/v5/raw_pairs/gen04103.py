from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand", "category")
    .agg(F.sum("price").alias("total_price"))
    .filter(F.col("total_price") < 3300)
)
