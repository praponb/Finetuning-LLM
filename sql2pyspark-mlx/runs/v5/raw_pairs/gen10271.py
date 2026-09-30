from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region", "city")
    .agg(F.avg("sqft").alias("avg_sqft"))
)
