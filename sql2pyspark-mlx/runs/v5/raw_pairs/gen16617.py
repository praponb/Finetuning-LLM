from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
    )
    .filter(F.col("stddev_sqft") >= 30)
    .select("region", "avg_sqft", "stddev_sqft")
    .orderBy(F.col("stddev_sqft").desc())
)
