from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").isNotNull())
    .select("price", F.explode("tags").alias("tag"))
    .groupBy("tag")
    .agg(F.round(F.avg("price"), 2).alias("avg_price"))
)
