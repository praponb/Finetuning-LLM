from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id")
    .pivot("brand", ["Stark", "Initech"])
    .agg(F.avg("price").alias("total"), F.count(F.lit(1)).alias("n"))
)
