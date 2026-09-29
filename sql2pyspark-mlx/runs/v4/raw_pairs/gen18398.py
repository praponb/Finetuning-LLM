from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") >= 5902)
    .groupBy("region")
    .agg(F.avg("sqft").alias("avg_sqft"))
    .orderBy("region")
    .limit(10)
)
