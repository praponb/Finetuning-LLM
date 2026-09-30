from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.count("sqft").alias("sqft_count"),
    )
    .orderBy("avg_sqft", F.col("sqft_count").desc())
)
