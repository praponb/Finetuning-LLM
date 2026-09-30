from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") == "Bangkok") & (F.col("sqft") >= 9388))
    .groupBy("region")
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
    )
    .filter(F.col("avg_sqft") > 48)
    .orderBy("region", F.col("sqft_count").desc())
)
