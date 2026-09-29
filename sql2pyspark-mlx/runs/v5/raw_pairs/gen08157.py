from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(6846, 12072) & F.col("sqft").between(9137, 10292))
    .groupBy("city", "region")
    .agg(F.round(F.avg("sqft"), 2).alias("avg_sqft"))
    .orderBy("region", "city")
)
