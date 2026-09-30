from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(6013, 14923))
    .groupBy("region")
    .agg(F.max("sqft").alias("max_sqft"))
)
