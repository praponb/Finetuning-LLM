from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .pivot("city", ["Singapore", "Berlin"])
    .agg(F.sum("sqft").alias("total"), F.count(F.lit(1)).alias("cnt"))
)
