from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(F.avg("sqft").alias("avg_sqft"))
    .filter(F.col("avg_sqft") < 15300)
    .orderBy(F.col("avg_sqft").desc(), "region")
    .limit(1)
)
