from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .pivot("region", ["East", "West", "North"])
    .agg(F.max("sqft").alias("amount"), F.count(F.lit(1)).alias("cnt"))
)
