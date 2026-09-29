from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(F.avg("sqft").alias("avg_sqft"))
    .filter(F.col("avg_sqft") >= 14000)
)
