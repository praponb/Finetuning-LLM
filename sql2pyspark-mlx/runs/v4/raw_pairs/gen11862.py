from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.min("sqft").alias("min_sqft"),
    )
    .filter(F.col("min_sqft") > 6500)
    .select("region", "avg_sqft", "min_sqft")
)
