from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .pivot("region", ["North", "South", "East"])
    .agg(F.sum("sqft").alias("total"), F.count(F.lit(1)).alias("n"))
)
