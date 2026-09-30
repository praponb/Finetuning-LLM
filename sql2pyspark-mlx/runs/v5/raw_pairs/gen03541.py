from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(109, 217))
    .select("price", F.explode("tags").alias("tag_name"))
    .groupBy("tag_name")
    .agg(F.round(F.avg("price"), 2).alias("avg_price"))
    .orderBy(F.col("avg_price").desc(), "tag_name")
)
