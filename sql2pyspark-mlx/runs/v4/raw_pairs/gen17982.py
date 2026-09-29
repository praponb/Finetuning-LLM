from pyspark.sql import functions as F

result = (
    df_stores
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("avg_sqft").desc())
)
