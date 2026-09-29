from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id", "category")
    .pivot("brand", ["Stark", "Initech"])
    .agg(F.avg("price").alias("value_sum"), F.count(F.lit(1)).alias("n"))
)
