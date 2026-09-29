from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 17189, "high").when(F.col("sqft") >= 8158, "mid").otherwise("low").alias("sqft_bucket"),
        "city",
    )
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.count("*").alias("cnt"),
    )
)
