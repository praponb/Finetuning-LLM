from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 14086, "high").when(F.col("sqft") >= 3916, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.avg("sqft").alias("avg_sqft"))
    .filter(F.col("avg_sqft") > 5800)
    .orderBy(F.col("sqft_bucket").desc())
    .limit(1)
)
